# Sensor waiting for file
from airflow.decorators import dag, task
from datetime import datetime
from airflow.sensors.filesystem import FileSensor

@dag(
    schedule=None,
    start_date=datetime(2024,7,10),
    tags=['sensors'],
    catchup=False
)

def sensor_wait_for_file_dag():
    wait_for_file=FileSensor.partial(
        task_id='wait_for_file',
        fs_conn_id='fs_default',
        mode='reschedule',
        poke_interval=60,
        timeout= 1 * 24 * 60 * 60
    ).expand(
        filepath=['data1.csv','data2.csv','data3.csv']        
    )

    @task
    def process_file():
        print("File processed")

    wait_for_file >> process_file()

sensor_wait_for_file_dag()