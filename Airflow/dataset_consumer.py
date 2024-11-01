from airflow import DAG, Dataset
from airflow.decorators import task
from datetime import datetime

dataset = Dataset("/tmp/my_file.txt")

with DAG(
   dag_id='consumer',
   schedule=[dataset],
   start_date=datetime(2022, 1, 1),
   catchup=False):

   @task
   def read_dataset(triggering_dataset_events=None):
       print(triggering_dataset_events)
       with open(dataset.uri, 'r') as f:
           print(f.read())
   read_dataset()

