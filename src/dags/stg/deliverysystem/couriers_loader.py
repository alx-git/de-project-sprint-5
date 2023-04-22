from logging import Logger
from typing import List

from lib.api_connect import ApiConnect
from lib.pg_connect import PgConnect
from lib.dict_util import json2str
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel


class CourierObj(BaseModel):
    json_value: str


class CourierOriginRepository:
    def __init__(self, api: ApiConnect) -> None:
        self._db = api

    def list_couriers(self, limit, offset) -> List[CourierObj]:
        return self._db.client(limit, offset)


class CourierDestRepository:

    def insert_courier(self, conn: Connection, courier: CourierObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO stg.deliverysystem_couriers(json_value)
                    VALUES (%(json_value)s)
                    ON CONFLICT (json_value) DO UPDATE
                    SET
                        json_value = EXCLUDED.json_value;
                """,
                {
                    "json_value": courier
                },
            )


class CourierLoader:
    LIMIT = 50
    
    def __init__(self, api_origin: ApiConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = CourierOriginRepository(api_origin)
        self.stg = CourierDestRepository()
        self.log = log

    def load_couriers(self):
        with self.pg_dest.connection() as conn:
            count = 0
            while True:
                load_queue = self.origin.list_couriers(self.LIMIT, self.LIMIT*count)
                if not load_queue:
                    break
                for courier in load_queue:
                    self.stg.insert_courier(conn, json2str(courier))
                count += 1