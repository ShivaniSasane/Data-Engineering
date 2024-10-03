# Create variables
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.models import Variable
from datetime import datetime

def test_var_task_func(ml_parameter):
   print(ml_parameter)

with DAG('variable_example_dag', start_date=datetime(2024, 8, 20),
   schedule_interval='@weekly', catchup=False) as dag:
   var_tasks_list = []
   for ml_parameter in Variable.get('ml_model_parameters', deserialize_json=True)["param"]:
       var_tasks_list.append(PythonOperator(
           task_id=f'var_test_task_{ml_parameter}',
           python_callable=test_var_task_func,
           op_kwargs={
               'ml_parameter': ml_parameter
           }
       ))

   report_task = BashOperator(
       task_id='report_task',
       bash_command='echo "report_{{ var.value.ml_report_name }}"'
   )

   var_tasks_list >> report_task       