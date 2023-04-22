import logging

import pendulum
from airflow.decorators import dag, task
from stg.bonussystem.ranks_loader import RankLoader
from stg.bonussystem.users_loader import UserLoader
from stg.bonussystem.event_loader import EventLoader
from lib import ConnectionBuilder

log = logging.getLogger(__name__)


@dag(
    schedule_interval='0/15 * * * *',
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),
    catchup=False,
    tags=['stg'],
    is_paused_upon_creation=True
)
def bonussystem_dag():
    # Создаем подключение к базе dwh.
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    # Создаем подключение к базе подсистемы бонусов.
    origin_pg_connect = ConnectionBuilder.pg_conn("PG_ORIGIN_BONUS_SYSTEM_CONNECTION")

    @task(task_id="ranks_load")
    def load_ranks():
        rest_loader = RankLoader(origin_pg_connect, dwh_pg_connect, log)
        rest_loader.load_ranks()

    @task(task_id="users_load")
    def load_users():
        rest_loader = UserLoader(origin_pg_connect, dwh_pg_connect, log)
        rest_loader.load_users()

    @task(task_id="event_load")
    def load_events():
        rest_loader = EventLoader(origin_pg_connect, dwh_pg_connect, log)
        rest_loader.load_events()


    ranks_dict = load_ranks()
    users_dict = load_users()
    events_dict = load_events()

    ranks_dict
    users_dict
    events_dict


bonussystem_dag_run = bonussystem_dag()
