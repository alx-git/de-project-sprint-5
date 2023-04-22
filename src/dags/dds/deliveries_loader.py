import json
from datetime import datetime
from datetime import time
from dateutil.parser import parse
from typing import List, Optional

from lib.pg_connect import PgConnect
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel

from dds.dds_settings_repository import DdsEtlSettingsRepository, EtlSetting


class DeliveryJsonObj(BaseModel):
    id: int
    json_value: str
    courier_id: int
    order_id: int
    

class DeliveryDdsObj(BaseModel):
    id: int
    delivery_id: str
    order_id: int
    courier_id: int
    delivery_ts: datetime
    address: str
    rate: int
    tip_sum: int


class DeliveryRawRepository:
    def load_raw_deliveries(self, conn: Connection, last_loaded_record_id: int) -> List[DeliveryJsonObj]:
        with conn.cursor(row_factory=class_row(DeliveryJsonObj)) as cur:
            cur.execute(
                """
                    SELECT dd.id, json_value, dc.id as courier_id, do2.id as order_id
                    FROM stg.deliverysystem_deliveries dd
                    JOIN dds.dm_couriers dc ON (dd.json_value::json ->> 'courier_id')=dc.courier_id
                    JOIN dds.dm_orders do2 ON (dd.json_value::json ->> 'order_id')=do2.order_key
                    WHERE dd.id > %(last_loaded_record_id)s;
                """,
                {"last_loaded_record_id": last_loaded_record_id},
            )
            objs = cur.fetchall()
        return objs


class DeliveryDdsRepository:
    def insert_delivery(self, conn: Connection, delivery: DeliveryDdsObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.dm_deliveries(delivery_id, order_id, courier_id, delivery_ts, address, rate, tip_sum)
                    VALUES (%(delivery_id)s, %(order_id)s, %(courier_id)s, %(delivery_ts)s, %(address)s, %(rate)s, %(tip_sum)s);
                """,
                {
                    "delivery_id": delivery.delivery_id,
                    "order_id": delivery.order_id,
                    "courier_id": delivery.courier_id,
                    "delivery_ts": delivery.delivery_ts,
                    "address": delivery.address,
                    "rate": delivery.rate,
                    "tip_sum": delivery.tip_sum,
                },
            )

    def get_delivery(self, conn: Connection, delivery_id: str):
        with conn.cursor() as cur:
            cur.execute(
                """
                    SELECT *
                    FROM dds.dm_deliveries
                    WHERE delivery_id = %(delivery_id)s;
                """,
                {"delivery_id": delivery_id},
            )
            obj = cur.fetchone()
        return obj


class DeliveryLoader:
    WF_KEY = "deliveries_raw_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_delivery_id"

    def __init__(self, pg: PgConnect, settings_repository: DdsEtlSettingsRepository) -> None:
        self.dwh = pg
        self.raw = DeliveryRawRepository()
        self.dds = DeliveryDdsRepository()
        self.settings_repository = settings_repository

    def parse_deliveries(self, raws: List[DeliveryJsonObj]) -> List[DeliveryDdsObj]:
        res = []
        for r in raws:
            delivery_json = json.loads(r.json_value)
            t = DeliveryDdsObj(id=r.id,
                           delivery_id=delivery_json['delivery_id'],
                           order_id=r.order_id,
                           courier_id=r.courier_id,
                           delivery_ts=parse(delivery_json['delivery_ts']),
                           address=delivery_json['address'],
                           rate=delivery_json['rate'],
                           tip_sum=delivery_json['tip_sum']

                           )

            res.append(t)
        return res

    def load_deliveries(self):
        with self.dwh.connection() as conn:
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.LAST_LOADED_ID_KEY: -1})

            last_loaded_id = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]

            load_queue = self.raw.load_raw_deliveries(conn, last_loaded_id)
            load_queue.sort(key=lambda x: x.id)
            deliveries_to_load = self.parse_deliveries(load_queue)
            for u in deliveries_to_load:
                existing = self.dds.get_delivery(conn, u.delivery_id)
                if not existing:
                    self.dds.insert_delivery(conn, u)

                wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = u.id
                self.settings_repository.save_setting(conn, wf_setting)
