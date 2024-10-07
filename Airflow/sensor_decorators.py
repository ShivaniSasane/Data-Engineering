# Create sensors usng sensor decorators
from airflow.decorators import dag, task
from datetime import datetime
import requests
from airflow.sensors.base import PokeReturnValue

@dag(start_date=datetime(2024, 7, 1), schedule=None, catchup=False, tags=['sensors'])
def sensor_decorator():

    @task.sensor(poke_interval=30, timeout=3600, mode="poke")
    def check_connection_status() -> PokeReturnValue:
        r = requests.get("https://registry.astronomer.io/providers/apache-airflow/versions/latest")
        print(r.status_code)

        if r.status_code == 200:
            condition_met = True
            operator_return_value = "Sample return value using xcom"
        else:
            condition_met = False
            operator_return_value = None
            print(f"Astro registry URL returned the status code {r.status_code}")

        return PokeReturnValue(is_done=condition_met, xcom_value=operator_return_value)

    @task
    def print_connection_status(return_val):
        print(return_val)

    print_connection_status(check_connection_status())

sensor_decorator()