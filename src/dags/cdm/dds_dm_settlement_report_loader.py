from datetime import date
from typing import List, Optional

from lib.pg_connect import PgConnect
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel


class ReportSettlementObj(BaseModel):
    restaurant_id: str
    restaurant_name: str
    settlement_date: date
    orders_count: int
    orders_total_sum: float
    orders_bonus_payment_sum: float
    orders_bonus_granted_sum: float
    order_processing_fee: float
    restaurant_reward_sum: float


class ReportSettlementDdsRepository:
    def select_data(self, conn: Connection) -> List[ReportSettlementObj]:
        with open('/lessons/dags/cdm/dds_dm_settlement_report.sql', 'r') as f:
            sql_code = f.read()
        with conn.cursor(row_factory=class_row(ReportSettlementObj)) as cur:
            cur.execute(sql_code)
            objs = cur.fetchall()
        return objs


class ReportSettlementCdmRepository:
    def insert_report(self, conn: Connection, report: ReportSettlementObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO cdm.dm_settlement_report (restaurant_id, restaurant_name, settlement_date, orders_count, orders_total_sum, orders_bonus_payment_sum, orders_bonus_granted_sum, order_processing_fee, restaurant_reward_sum)
                    VALUES (%(restaurant_id)s, %(restaurant_name)s, %(settlement_date)s, %(orders_count)s, %(orders_total_sum)s, %(orders_bonus_payment_sum)s, %(orders_bonus_granted_sum)s, %(order_processing_fee)s,  %(restaurant_reward_sum)s)
                    ON CONFLICT (restaurant_id, settlement_date) DO UPDATE SET
                    orders_count=excluded.orders_count,
                    orders_total_sum=excluded.orders_total_sum,
                    orders_bonus_payment_sum=excluded.orders_bonus_payment_sum,
                    orders_bonus_granted_sum=excluded.orders_bonus_granted_sum,
                    order_processing_fee=excluded.order_processing_fee,
                    restaurant_reward_sum=excluded.restaurant_reward_sum;
                """,
                {
                    "restaurant_id": report.restaurant_id,
                    "restaurant_name": report.restaurant_name,
                    "settlement_date": report.settlement_date,
                    "orders_count": report.orders_count,
                    "orders_total_sum": report.orders_total_sum,
                    "orders_bonus_payment_sum": report.orders_bonus_payment_sum,
                    "orders_bonus_granted_sum": report.orders_bonus_granted_sum,
                    "order_processing_fee": report.order_processing_fee,
                    "restaurant_reward_sum": report.restaurant_reward_sum,
                },
            )


class ReportSettlementLoader:
   
    def __init__(self, pg: PgConnect) -> None:
        self.dwh = pg
        self.dds = ReportSettlementDdsRepository()
        self.cdm = ReportSettlementCdmRepository()

    def load_report(self):
        with self.dwh.connection() as conn:
            load_queue = self.dds.select_data(conn)
            for q in load_queue:
                self.cdm.insert_report(conn, q)

