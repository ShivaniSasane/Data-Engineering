# created to test if work slots are available after other sensor dags are running
from airflow.decorators import dag, task
from airflow.sensors.filesystem import FileSensor
from datetime import datetime

@dag(
    schedule=None,
    start_date=datetime(2024, 7, 20),
    tags=['sensors'],
    catchup=False
)
def test_sensor_slot_dag():

    @task
    def test_run():
        print("This DAG ran!")

    test_run()

test_sensor_slot_dag()