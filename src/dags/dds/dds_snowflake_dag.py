import logging

import pendulum
from airflow import DAG
from airflow.decorators import task
from lib.pg_connect import ConnectionBuilder

from dds.dds_settings_repository import DdsEtlSettingsRepository
from dds.fct_products_loader import FctProductsLoader
from dds.order_loader import OrderLoader
from dds.products_loader import ProductLoader
from dds.user_loader import UserLoader
from dds.restaurant_loader import RestaurantLoader
from dds.timestamp_loader import TimestampLoader
from dds.products_loader import ProductLoader
from dds.order_loader import OrderLoader
from dds.courier_loader import CourierLoader
from dds.deliveries_loader import DeliveryLoader



log = logging.getLogger(__name__)

with DAG(
    dag_id='dds',
    schedule_interval='0/15 * * * *',
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),
    catchup=False,
    tags=['dds'],
    is_paused_upon_creation=False
) as dag:
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    settings_repository = DdsEtlSettingsRepository()

    @task(task_id="dm_restaurants_load")
    def load_dm_restaurants(ds=None, **kwargs):
        rest_loader = RestaurantLoader(dwh_pg_connect, settings_repository)
        rest_loader.load_restaurants()
    
    @task(task_id="dm_products_load")
    def load_dm_products(ds=None, **kwargs):
        prod_loader = ProductLoader(dwh_pg_connect, settings_repository)
        prod_loader.load_products()
    
    @task(task_id="dm_timestamps_load")
    def load_dm_timestamps(ds=None, **kwargs):
        ts_loader = TimestampLoader(dwh_pg_connect, settings_repository)
        ts_loader.load_timestamps()

    @task(task_id="dm_users_load")
    def load_dm_users(ds=None, **kwargs):
        user_loader = UserLoader(dwh_pg_connect, settings_repository)
        user_loader.load_users()

    @task(task_id="dm_orders_load")
    def load_dm_orders(ds=None, **kwargs):
        order_loader = OrderLoader(dwh_pg_connect, settings_repository)
        order_loader.load_order()
    
    @task(task_id="fct_order_products_load")
    def load_fct_order_products(ds=None, **kwargs):
        fct_loader = FctProductsLoader(dwh_pg_connect, settings_repository)
        fct_loader.load_product_facts()
    
    @task(task_id="dm_couriers_load")
    def load_dm_couriers(ds=None, **kwargs):
        courier_loader = CourierLoader(dwh_pg_connect, settings_repository)
        courier_loader.load_couriers()

    @task(task_id="dm_deliveries_load")
    def load_dm_deliveries(ds=None, **kwargs):
        deliveries_loader = DeliveryLoader(dwh_pg_connect, settings_repository)
        deliveries_loader.load_deliveries()

    dm_restaurants = load_dm_restaurants()
    dm_products = load_dm_products()
    dm_timestamps = load_dm_timestamps()
    dm_users = load_dm_users()
    dm_orders = load_dm_orders()
    fct_order_products = load_fct_order_products()
    dm_couriers = load_dm_couriers()
    dm_deliveries = load_dm_deliveries()

    dm_restaurants >> dm_products >> dm_timestamps >> dm_users >> dm_orders >> fct_order_products >> dm_couriers >> dm_deliveries
    