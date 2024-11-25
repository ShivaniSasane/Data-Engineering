from airflow.decorators import task, dag
from airflow.exceptions import AirflowException
from datetime import datetime

@dag(start_date=datetime(2024, 11, 1), schedule=None, catchup=False, tags=['cli'])
def cli_test_task():

    @task
    def my_task(val):
        #raise AirflowException()
        print(val)
        return 42

    my_task(80)
cli()