from airflow import DAG

from airflow.decorators import task

from datetime import datetime

import random

with DAG(dag_id='my_dag', start_date=datetime(2022, 1, 1), schedule_interval='@daily', catchup=False) as dag:

   @task

   def get_files():

       return [f"file_{nb}" for nb in range(random.randint(3, 5))]

   @task

   def download_file(folder: str, file: str):

       return f"{folder}/{file}"

   files = download_file.partial(folder='/usr/local').expand(file=get_files())