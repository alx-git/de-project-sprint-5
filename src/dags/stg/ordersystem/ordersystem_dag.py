import logging

import pendulum
from airflow.decorators import dag, task
from airflow.models.variable import Variable
from stg.ordersystem.pg_saver import PgSaverOrders, PgSaverRestaurants, PgSaverUsers
from stg.ordersystem.order_loader import OrderLoader
from stg.ordersystem.order_reader import OrderReader
from stg.ordersystem.restaurant_loader import RestaurantLoader
from stg.ordersystem.restaurant_reader import RestaurantReader
from stg.ordersystem.user_loader import UserLoader
from stg.ordersystem.user_reader import UserReader

from lib.mongo_connect import MongoConnect
from lib.pg_connect import ConnectionBuilder

log = logging.getLogger(__name__)


@dag(
    schedule_interval='0/15 * * * *',
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),
    catchup=False,
    tags=['stg'],
    is_paused_upon_creation=True
)
def ordersystem_dag():
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")
    cert_path = Variable.get("MONGO_DB_CERTIFICATE_PATH")
    db_user = Variable.get("MONGO_DB_USER")
    db_pw = Variable.get("MONGO_DB_PASSWORD")
    rs = Variable.get("MONGO_DB_REPLICA_SET")
    db = Variable.get("MONGO_DB_DATABASE_NAME")
    host = Variable.get("MONGO_DB_HOST")

    @task()
    def load_orders():
        pg_saver = PgSaverOrders()
        mongo_connect = MongoConnect(cert_path, db_user, db_pw, host, rs, db, db)
        collection_reader = OrderReader(mongo_connect)
        loader = OrderLoader(collection_reader, dwh_pg_connect, pg_saver, log)
        loader.run_copy()

    @task()
    def load_restaurants():
        pg_saver = PgSaverRestaurants()
        mongo_connect = MongoConnect(cert_path, db_user, db_pw, host, rs, db, db)
        collection_reader = RestaurantReader(mongo_connect)
        loader = RestaurantLoader(collection_reader, dwh_pg_connect, pg_saver, log)
        loader.run_copy()

    @task()
    def load_users():
        pg_saver = PgSaverUsers()
        mongo_connect = MongoConnect(cert_path, db_user, db_pw, host, rs, db, db)
        collection_reader = UserReader(mongo_connect)
        loader = UserLoader(collection_reader, dwh_pg_connect, pg_saver, log)
        loader.run_copy()
    
    order_loader = load_orders()
    restaurant_loader = load_restaurants()
    user_loader = load_users()

    order_loader
    restaurant_loader
    user_loader

order_stg_dag = ordersystem_dag()