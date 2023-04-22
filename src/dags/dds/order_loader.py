import json
from typing import List, Optional

from lib.pg_connect import PgConnect
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel

from dds.dds_settings_repository import DdsEtlSettingsRepository, EtlSetting


class OrderJsonObj(BaseModel):
    id: int
    object_id: str
    object_value: str
    restaurant_id: int
    timestamp_id: int
    user_id: int


class OrderDdsObj(BaseModel):
    id: int
    order_key: str
    order_status: str
    restaurant_id: int
    timestamp_id: int
    user_id: int


class OrderRawRepository:
    def load_raw_order(self, conn: Connection, last_loaded_record_id: int) -> List[OrderJsonObj]:
        with conn.cursor(row_factory=class_row(OrderJsonObj)) as cur:
            cur.execute(
                """
                    select oo.id as id,
                            oo.object_id as object_id ,
                            oo.object_value as object_value ,
                            dr.id as restaurant_id,
                            dt.id as timestamp_id,
                            du.id as user_id
                     FROM stg.ordersystem_orders oo
                     join dds.dm_restaurants dr 
                     on dr.restaurant_id =  ((oo.object_value::json ->> 'restaurant')::json ->> 'id')   
                    join dds.dm_timestamps dt 
                     on dt.ts = (oo.object_value::json ->> 'date')::timestamp
                    join dds.dm_users du 
                    on du.user_id = ((oo.object_value::json ->> 'user')::json ->> 'id')
                    WHERE oo.id > %(last_loaded_record_id)s;
                """,
                {"last_loaded_record_id": last_loaded_record_id},
            )
            objs = cur.fetchall()
        return objs


class OrderDdsRepository:
    def insert_order(self, conn: Connection, order: OrderDdsObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.dm_orders(order_key, order_status, restaurant_id, timestamp_id, user_id)
                    VALUES (%(order_key)s, %(order_status)s, %(restaurant_id)s, %(timestamp_id)s, %(user_id)s);
                """,
                {
                    "order_key": order.order_key,
                    "order_status": order.order_status,
                    "restaurant_id": order.restaurant_id,
                    "timestamp_id":order.timestamp_id,
                    "user_id":order.user_id
                },
            )

    def get_order(self, conn: Connection, order_key: str) -> Optional[OrderDdsObj]:
        with conn.cursor(row_factory=class_row(OrderDdsObj)) as cur:
            cur.execute(
                """
                    SELECT
                        id,
                        order_key,
                        order_status,
                        restaurant_id,
                        timestamp_id,
                        user_id
                    FROM dds.dm_orders
                    WHERE order_key = %(order_key)s;
                """,
                {"order_key": order_key},
            )
            obj = cur.fetchone()
        return obj


class OrderLoader:
    WF_KEY = "order_raw_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_order_key"

    def __init__(self, pg: PgConnect, settings_repository: DdsEtlSettingsRepository) -> None:
        self.dwh = pg
        self.raw = OrderRawRepository()
        self.dds = OrderDdsRepository()
        self.settings_repository = settings_repository

    def parse_order(self, raws: List[OrderJsonObj]) -> List[OrderDdsObj]:
        res = []
        for r in raws:
            order_json = json.loads(r.object_value)
            t = OrderDdsObj(id=0,
                           order_key=order_json['_id'],
                           order_status=order_json['final_status'],
                           restaurant_id = r.restaurant_id,
                           timestamp_id = r.timestamp_id,
                           user_id = r.user_id
                           )

            res.append(t)
        return res


    def load_order(self):
        with self.dwh.connection() as conn:
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.LAST_LOADED_ID_KEY: -1})

            last_loaded_id = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]

            load_queue = self.raw.load_raw_order(conn, last_loaded_id)
            load_queue.sort(key=lambda x: x.id)
            order_to_load = self.parse_order(load_queue)
            for u in order_to_load:
                existing = self.dds.get_order(conn, u.order_key)
                if not existing:
                    self.dds.insert_order(conn, u)

                wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = u.id
                self.settings_repository.save_setting(conn, wf_setting)
