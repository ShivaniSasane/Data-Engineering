from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from datetime import datetime

@dag(schedule=None, tags=['branching'], catchup=False, start_date=datetime(2024,9,20))
def branching_python_op():
    @task.branch(task_id="branching_py_task")
    def branch_function(return_value):
        if return_value >=5:
            return "continue_task"
        elif return_value >=3:
            return "stop_task"
        else:
            return "join_task"
    
    @task
    def branching_value_calc():
        return 6
    
    continue_op = EmptyOperator(task_id="continue_task")
    stop_op = EmptyOperator(task_id="stop_task")
    join_op = EmptyOperator(task_id="join_task")

    branch_op = branch_function(branching_value_calc())

    branch_op >> [continue_op,stop_op,join_op]
    #continue_op >> join_op     # this will skip join_task execution if continue_task is skipped even if join_task condition is True

dag = branching_python_op()