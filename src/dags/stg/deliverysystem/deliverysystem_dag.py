import logging

import pendulum
from airflow.decorators import dag, task
from airflow.models.variable import Variable
from stg.deliverysystem.couriers_loader import CourierLoader
from stg.deliverysystem.deliveries_loader import DeliveryLoader

from lib.api_connect import ApiConnect
from lib.pg_connect  import ConnectionBuilder

log = logging.getLogger(__name__)


@dag(
    schedule_interval='0/15 * * * *',
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),
    catchup=False,
    tags=['stg'],
    is_paused_upon_creation=True
)
def deliverysystem_dag():
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")
    headers = {'X-Nickname': 'kovalchukalexander', 'X-Cohort': '8', 'X-API-KEY': '25c27781-8fde-4b30-a22e-524044a7580f'}
    
    @task()
    def load_couriers():
        api_connect = ApiConnect(headers, '_id', 'asc', 'couriers')
        loader = CourierLoader(api_connect, dwh_pg_connect, log)
        loader.load_couriers()

    @task()
    def load_deliveries():
        api_connect = ApiConnect(headers, '_id', 'asc', 'deliveries')
        loader = DeliveryLoader(api_connect, dwh_pg_connect, log)
        loader.load_deliveries()


    couriers_loader = load_couriers()
    deliveries_loader = load_deliveries()
    couriers_loader
    deliveries_loader


deliverysystem_stg_dag = deliverysystem_dag()