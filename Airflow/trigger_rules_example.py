from airflow import DAG
from airflow.decorators import task
from airflow.exceptions import AirflowSkipException, AirflowFailException
from datetime import datetime, timedelta

with DAG('trigger_rules_example', description='implement trigger rules', tags=['trigger_rules'],
   start_date=datetime(2024, 8, 30),
   schedule_interval='@daily', catchup=False) as dag:

    @task(trigger_rule='all_success')
    def task1():
       raise AirflowFailException       

    @task(trigger_rule='one_failed')
    def task2():
       raise AirflowSkipException

    @task(trigger_rule='one_failed')
    def notify():
       None

    task1() >> [ task2(), notify()]