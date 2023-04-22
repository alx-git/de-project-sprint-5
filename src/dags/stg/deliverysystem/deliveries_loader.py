import datetime
from dateutil.parser import parse
from logging import Logger
from typing import List


from stg.stg_settings_repository import EtlSetting, StgEtlSettingsRepository
from lib.api_connect import ApiConnect
from lib.pg_connect import PgConnect
from lib.dict_util import json2str
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel


class DeliveryObj(BaseModel):
    json_value: str


class DeliveryOriginRepository:
    def __init__(self, api: ApiConnect) -> None:
        self._db = api

    def list_deliveries(self, limit, offset, from_date) -> List[DeliveryObj]:
        return self._db.client(limit, offset, from_date)


class DeliveryDestRepository:

    def insert_delivery(self, conn: Connection, delivery: DeliveryObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO stg.deliverysystem_deliveries(json_value)
                    VALUES (%(json_value)s)
                    ON CONFLICT (json_value) DO UPDATE
                    SET
                        json_value = EXCLUDED.json_value;
                """,
                {
                    "json_value": delivery
                },
            )


class DeliveryLoader:
    
    WF_KEY = "deliveries_to_stg_workflow"
    LAST_LOADED_TS_KEY = "last_loaded_ts"
    LIMIT = 50

    def __init__(self, api_origin: ApiConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = DeliveryOriginRepository(api_origin)
        self.stg = DeliveryDestRepository()
        self.settings_repository = StgEtlSettingsRepository()
        self.log = log

    def load_deliveries(self) -> int:
        with self.pg_dest.connection() as conn:

            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(
                    id=0,
                    workflow_key=self.WF_KEY,
                    workflow_settings={
                        self.LAST_LOADED_TS_KEY: (datetime.date.today()-datetime.timedelta(days=7)).isoformat()
                    }
                )

            last_loaded_ts_str = wf_setting.workflow_settings[self.LAST_LOADED_TS_KEY]
            last_loaded_ts = datetime.datetime.fromisoformat(last_loaded_ts_str)
            self.log.info(f"starting to load from last checkpoint: {last_loaded_ts}")

            count = 0
            while True:
                load_queue = self.origin.list_deliveries(self.LIMIT, self.LIMIT*count, last_loaded_ts)
                if not load_queue:
                    break
                for delivery in load_queue:
                    self.stg.insert_delivery(conn, json2str(delivery))
                print(load_queue)
                wf_setting.workflow_settings[self.LAST_LOADED_TS_KEY] = parse(max([t["delivery_ts"] for t in load_queue]))
                wf_setting_json = json2str(wf_setting.workflow_settings)
                self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)
                count += 1
   

          