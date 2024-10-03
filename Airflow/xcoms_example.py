#Create xcoms to transfer values from 1 task to another
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def task_transform(ti):
   import requests
   resp = requests.get('https://swapi.dev/api/people/1').json()
   print(resp)
   person = {}
   person["height"] = int(resp["height"]) - 20
   person["mass"] = int(resp["mass"]) - 50
   person["hair_color"] = "black" if resp["hair_color"] == "blond" else "blond"
   person["eye_color"] = "hazel" if resp["eye_color"] == "blue" else "blue"
   person["gender"] = "female" if resp["gender"] == "male" else "female"
   ti.xcom_push("character_info", person)

def task_transform_2(ti):
   import requests
   resp = requests.get(f'https://swapi.dev/api/people/2').json()
   print(resp)
   person = {}
   person["height"] = int(resp["height"]) - 50
   person["mass"] = int(resp["mass"]) - 20
   person["hair_color"] = "burgundy" if resp["hair_color"] == "blond" else "brown"
   person["eye_color"] = "green" if resp["eye_color"] == "blue" else "black"
   person["gender"] = "male" if resp["gender"] == "male" else "female"
   ti.xcom_push("character_info", person)

def task_load(ti):
   print(ti.xcom_pull(key = 'character_info',task_ids = ['task_transform','task_transform_2']))

with DAG(
   'xcoms_example',
   schedule = None,
   start_date = datetime(2024,8,10),
   catchup = False
   ):

   t1 = PythonOperator(
       task_id = 'task_transform',
       python_callable = task_transform
   )

   t2 = PythonOperator(
       task_id = 'load',
       python_callable = task_load
   )
   t3 = PythonOperator(
       task_id = 'task_transform_2',
       python_callable = task_transform_2
   )

   [t1,t3] >> t2

   