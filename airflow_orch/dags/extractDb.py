from airflow.sdk import dag , task
from loguru import logger
from pendulum import *
from airflow.timetables.trigger import CronPartitionTimetable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from Services.ExtractDATA import ExtractDataFromDB
timezone = timezone("Africa/Casablanca")
@dag(
    start_date=datetime(year=2026,month=7,day=5,tz=timezone),
    end_date=datetime(year=2026,month=7,day=7,tz=timezone),
    catchup=True,
    # At 17:09 on every day-of-week from Sunday through Saturday in every month from January through December.
    schedule= CronPartitionTimetable("9 17 * JAN-DEC SUN-SAT",timezone=timezone)
)
def dag_donne_db():
    @task
    def orderDb():
        ExtractDataFromDB.check_table("order_connection","Orders","order")
    

    #gestion dependencise
    order_db = orderDb()
    [orderDb]
dag_donne_db()