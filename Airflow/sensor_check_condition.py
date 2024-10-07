# Create condition based sensors
from airflow import DAG
from datetime import datetime
from airflow.sensors.python import PythonSensor

def check_condition():
    a,b=20,30
    if a < b:
        return True
    else:
        return False

with DAG(
    dag_id="check_condition_sensor",
    start_date=datetime(2025, 2, 24),
    schedule="@daily",
    tags=['sensors'],
    catchup=False ):

    waiting_for_condition = PythonSensor(
        task_id="waiting_for_condition",
        python_callable=check_condition,
        poke_interval=30, 
        timeout= 24 * 60 * 60
    )

    waiting_for_condition