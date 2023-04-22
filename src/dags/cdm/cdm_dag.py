import logging

import pendulum
from airflow import DAG
from airflow.decorators import task
from lib.pg_connect import ConnectionBuilder

from cdm.dds_dm_courier_ledger import ReportCourierLoader
from cdm.dds_dm_settlement_report_loader import ReportSettlementLoader


log = logging.getLogger(__name__)

with DAG(
    dag_id='cdm',
    schedule_interval='0/15 * * * *',
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),
    catchup=False,
    tags=['cdm'],
    is_paused_upon_creation=False
) as dag:
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    @task(task_id="cdm_courier_report_load")
    def load_cdm_courier_report(ds=None, **kwargs):
        rest_loader = ReportCourierLoader(dwh_pg_connect)
        rest_loader.load_report()

    @task(task_id="cdm_settlement_report_load")
    def load_cdm_settlement_report(ds=None, **kwargs):
        rest_loader = ReportSettlementLoader(dwh_pg_connect)
        rest_loader.load_report()

    cdm_dm_courier_report = load_cdm_courier_report()
    cdm_dm_settlement_report = load_cdm_settlement_report()

    cdm_dm_courier_report
    cdm_dm_settlement_report
    