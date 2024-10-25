# Checks if current date in in range of target_lower and target_upper dates

from airflow.decorators import dag
from airflow.operators.datetime import BranchDateTimeOperator
from airflow.operators.weekday import BranchDayOfWeekOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime
from airflow.utils.weekday import WeekDay

@dag(schedule=None, start_date=datetime(2024,8,20), tags=['branching'],
     catchup=False
)
def branching_datetime():
    empty_task1 = EmptyOperator(task_id="date_in_range")
    empty_task2 = EmptyOperator(task_id="date_outside_range")
    empty_task3 = EmptyOperator(task_id="another_task")

    condition_task = BranchDayOfWeekOperator (
    #BranchDateTimeOperator(
        task_id="datetime_branch",
        follow_task_ids_if_true=['date_in_range','another_task'],
        follow_task_ids_if_false=['date_outside_range'],
        #target_lower=datetime(2024,8,25),
        #target_upper=datetime(2024,8,20)
        week_day={WeekDay.TUESDAY,WeekDay.FRIDAY}

    )

    condition_task >> [empty_task1,empty_task2,empty_task3]

dag=branching_datetime()