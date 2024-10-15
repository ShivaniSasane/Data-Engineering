#Sensor that waits for s3 files
from airflow.decorators import dag, task
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from datetime import datetime

@dag(
    schedule=None,
    start_date=datetime(2024,7,20),
    tags=['sensors'],
)

def s3_connection_sensor_dag():
    wait_for_s3_file=S3KeySensor(
        task_id='wait_for_s3_file',
        aws_conn_id="aws_s3",
        bucket_key="S3://",
        wildcard_match=True,
    )

    @task
    def process_s3_file():
        print("File processed")

    wait_for_s3_file >> process_s3_file()

s3_connection_sensor_dag()