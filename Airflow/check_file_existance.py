from airflow import DAG
from datetime import datetime
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
#from airflow.utils.helpers import chain

with DAG("check_file_existance", start_date=datetime(2025,2,23),
         description="Check if file exists using bash",
         schedule='@daily',
         tags=['Data Engineering'],
         catchup=False
         ):
    
    create_file = BashOperator(task_id='create_file',bash_command='echo "Hi there!" >/tmp/dummy.txt')

    check_file_existance = BashOperator(task_id='check_file_existance',bash_command='test -f /tmp/dummy.txt')

    read_file = PythonOperator(task_id='read_file',python_callable=lambda:print(open('/tmp/dummy.txt','rb').read()), retries=2)

    create_file >> check_file_existance >> read_file