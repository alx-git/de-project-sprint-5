from datetime import date
from typing import List, Optional

from lib.pg_connect import PgConnect
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel


class ReportCourierObj(BaseModel):
    courier_id: str
    courier_name: str
    settlement_year: int
    settlement_month: int
    orders_count: int
    orders_total_sum: float
    rate_avg: float
    order_processing_fee: float
    courier_order_sum: float
    courier_tips_sum: float
    courier_reward_sum: float


class ReportCourierDdsRepository:
    def select_data(self, conn: Connection) -> List[ReportCourierObj]:
        with open('/lessons/dags/cdm/dds_dm_courier_ledger.sql', 'r') as f:
            sql_code = f.read()
        with conn.cursor(row_factory=class_row(ReportCourierObj)) as cur:
            cur.execute(sql_code)
            objs = cur.fetchall()
        return objs


class ReportCourierCdmRepository:
    def insert_report(self, conn: Connection, report: ReportCourierObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO cdm.dm_courier_ledger (courier_id, courier_name, settlement_year, settlement_month, orders_count, orders_total_sum, rate_avg, order_processing_fee, courier_order_sum, courier_tips_sum, courier_reward_sum)
                    VALUES (%(courier_id)s, %(courier_name)s, %(settlement_year)s, %(settlement_month)s, %(orders_count)s, %(orders_total_sum)s, %(rate_avg)s, %(order_processing_fee)s,  %(courier_order_sum)s, %(courier_tips_sum)s,  %(courier_reward_sum)s)
                    ON CONFLICT (courier_id, settlement_year, settlement_month) DO UPDATE SET
                    courier_name=excluded.courier_name,
                    orders_count=excluded.orders_count,
                    orders_total_sum=excluded.orders_total_sum,
                    rate_avg=excluded.rate_avg,
                    order_processing_fee=excluded.order_processing_fee,
                    courier_order_sum=excluded.courier_order_sum,
                    courier_tips_sum=excluded.courier_tips_sum,
                    courier_reward_sum=excluded.courier_reward_sum;
                """,
                {
                    "courier_id": report.courier_id,
                    "courier_name": report.courier_name,
                    "settlement_year": report.settlement_year,
                    "settlement_month": report.settlement_month,
                    "orders_count": report.orders_count,
                    "orders_total_sum": report.orders_total_sum,
                    "rate_avg": report.rate_avg,
                    "order_processing_fee": report.order_processing_fee,
                    "courier_order_sum": report.courier_order_sum,
                    "courier_tips_sum": report.courier_tips_sum,
                    "courier_reward_sum": report.courier_reward_sum,
                },
            )


class ReportCourierLoader:
   
    def __init__(self, pg: PgConnect) -> None:
        self.dwh = pg
        self.dds = ReportCourierDdsRepository()
        self.cdm = ReportCourierCdmRepository()

    def load_report(self):
        with self.dwh.connection() as conn:
            load_queue = self.dds.select_data(conn)
            for q in load_queue:
                self.cdm.insert_report(conn, q)
