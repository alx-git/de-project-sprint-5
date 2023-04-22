import json
from typing import List, Optional

from lib.pg_connect import PgConnect
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel

from dds.dds_settings_repository import DdsEtlSettingsRepository, EtlSetting


class RestaurantJsonObj(BaseModel):
    id: int
    object_id: str
    object_value: str


class RestaurantDdsObj(BaseModel):
    id: int
    restaurant_id: str
    restaurant_name: str
    active_from: str
    active_to: str


class RestaurantRawRepository:
    def load_raw_restaurants(self, conn: Connection, last_loaded_record_id: int) -> List[RestaurantJsonObj]:
        with conn.cursor(row_factory=class_row(RestaurantJsonObj)) as cur:
            cur.execute(
                """
                    SELECT
                        id,
                        object_id,
                        object_value
                    FROM stg.ordersystem_restaurants
                    WHERE id > %(last_loaded_record_id)s;
                """,
                {"last_loaded_record_id": last_loaded_record_id},
            )
            objs = cur.fetchall()
        return objs


class RestaurantDdsRepository:
    def insert_restaurant(self, conn: Connection, restaurant: RestaurantDdsObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.dm_restaurants(restaurant_id, restaurant_name, active_from, active_to)
                    VALUES (%(restaurant_id)s, %(restaurant_name)s, to_timestamp(%(active_from)s,'YYYY-MM-DD hh24:mi:ss'), to_timestamp(%(active_to)s,'YYYY-MM-DD hh24:mi:ss'));
                """,
                {
                    "restaurant_id": restaurant.restaurant_id,
                    "restaurant_name": restaurant.restaurant_name,
                    "active_from": restaurant.active_from,
                    "active_to": restaurant.active_to
                },
            )

    def get_restaurant(self, conn: Connection, restaurant_id: str):
        with conn.cursor() as cur:
            cur.execute(
                """
                    SELECT
                        id,
                        restaurant_id,
                        restaurant_name,
                        to_char(active_from, 'YYYY-MM-DD hh24:mi:ss'),
                        to_char(active_to, 'YYYY-MM-DD hh24:mi:ss')
                    FROM dds.dm_restaurants
                    WHERE restaurant_id = %(restaurant_id)s;
                """,
                {"restaurant_id": restaurant_id},
            )
            obj = cur.fetchone()
        return obj


class RestaurantLoader:
    WF_KEY = "restaurants_raw_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_restaurant_id"

    def __init__(self, pg: PgConnect, settings_repository: DdsEtlSettingsRepository) -> None:
        self.dwh = pg
        self.raw = RestaurantRawRepository()
        self.dds = RestaurantDdsRepository()
        self.settings_repository = settings_repository

    def parse_restaurants(self, raws: List[RestaurantJsonObj]) -> List[RestaurantDdsObj]:
        res = []
        for r in raws:
            restaurant_json = json.loads(r.object_value)
            t = RestaurantDdsObj(id=r.id,
                           restaurant_id=restaurant_json['_id'],
                           restaurant_name=restaurant_json['name'],
                           active_from=restaurant_json['update_ts'],
                           active_to='2099-12-31 00:00:00.000'

                           )

            res.append(t)
        return res

    def load_restaurants(self):
        with self.dwh.connection() as conn:
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.LAST_LOADED_ID_KEY: -1})

            last_loaded_id = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]

            load_queue = self.raw.load_raw_restaurants(conn, last_loaded_id)
            load_queue.sort(key=lambda x: x.id)
            restaurants_to_load = self.parse_restaurants(load_queue)
            for u in restaurants_to_load:
                existing = self.dds.get_restaurant(conn, u.restaurant_id)
                if not existing:
                    self.dds.insert_restaurant(conn, u)

                wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = u.id
                self.settings_repository.save_setting(conn, wf_setting)
