from airflow import DAG
from airflow.decorators import task, branch_task
from datetime import datetime, timedelta

with DAG('trigger_rules_branching', 
   description='trigger rules using branching', 
   tags=['trigger_rules'],
   start_date=datetime(2024, 8, 30),
   schedule_interval='@daily', catchup=False) as dag:

   @branch_task
   def taskA():
       return 'taskB'

   @task(trigger_rule='all_success')
   def taskB():
       None

   @task(trigger_rule='all_success')
   def taskC():
       None

   @task(trigger_rule='none_failed_min_one_success')
   def taskD():
       None

   taskA() >> [taskB(), taskC()] >> taskD()