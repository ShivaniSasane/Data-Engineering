#from airflow.decorators import dag
from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator
from airflow.utils.helpers import chain


#@dag(description='sample desc', start_date=datetime(2024,7,23),tags=['sample'], schedule='@daily',catch=False)
#def sample_dag():
#    None
#sample_dag()

default_args ={
    'retries':3,
}

def print_something():
    print("This is my first DAG and first function to call")

def print_second():
    print("Second function to call")

def print_third():
    print("Third function to call")

def print_fourth():
    print("Fourth function to call")

def print_fifth():
    print("Fifth function to call")

with DAG('practice_dag', start_date=datetime(2024,7,23),   # if we don't use with context manager, then we have to use dag=dag in every task below
         default_args=default_args, # we can skip this and write separate args in tasks
         description='Sample Dag from tutorial',
         tags=['sample'],
         schedule='@daily',catchup=False):
    
    task_a = PythonOperator(task_id='task_a', python_callable=print_something)  # use pythin operator to create a task and call func inside task
    task_b = PythonOperator(task_id='task_b', python_callable=print_second)
    task_c = PythonOperator(task_id='task_c', python_callable=print_third) # can write retries=3 here as another param
    task_d = PythonOperator(task_id='task_d', python_callable=print_fourth)
    task_e = PythonOperator(task_id='task_e', python_callable=print_fifth)

    #task_a >> task_b >> [task_c, task_d]
    chain(task_a, [task_b, task_c], [task_d, task_e])

    #task_a >> task_b
