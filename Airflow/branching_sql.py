from airflow.decorators import dag
from airflow.providers.common.sql.operators.sql import BranchSQLOperator
import pendulum
from airflow.operators.empty import EmptyOperator

@dag(schedule_interval=None, start_date=pendulum.datetime(2024, 8, 10), catchup=False, tags=['branching'])
def branching_sql_dag():
   empty_task_11 = EmptyOperator(task_id="count_at_least_one")
   empty_task_22 = EmptyOperator(task_id="count_is_zero")
   empty_task_33 = EmptyOperator(task_id="one_more_task")

   count_rows = BranchSQLOperator(
       task_id="count_rows",
       conn_id="pg_default",
       sql="SELECT count(1) FROM customer_table",
       follow_task_ids_if_true=["count_at_least_one","one_more_task"],
       follow_task_ids_if_false="count_is_zero",
   )

   count_rows >> [empty_task_11, empty_task_22, empty_task_33]
dag = branching_sql_dag()
