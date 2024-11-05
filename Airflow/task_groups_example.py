from airflow.utils.task_group import TaskGroup
from airflow import DAG
from datetime import datetime
from airflow.operators.empty import EmptyOperator 

with DAG("task_group_example_dag", schedule=None, tags=['task_group'],catchup=False, start_date=datetime(2024,8,25)) as dag:

    #task1 = EmptyOperator(task_id="task1")

    groups=[]
    for gp_id in range(1,4):
        with TaskGroup(group_id=f'group{gp_id}') as g1:
            task2 = EmptyOperator(task_id="task2")
            task3 = EmptyOperator(task_id="task3")

            task2 >> task3

            # Task group definition ends here
            if gp_id == 'group1':
                task4 = EmptyOperator(task_id="task4")
                task2 >> task4
            
            groups.append(g1)

    [groups[0],groups[1]] >> groups[2]
    #task1 >> group1 >> task4
    
