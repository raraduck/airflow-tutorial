from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta

default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 12, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

dag = DAG(
    "taskgroup_pipeline",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False
)

def task1_func(level):
    print(f"{level} - task1 executed")

def task2_func(level):
    print(f"{level} - task2 executed")

with dag:
    start = DummyOperator(task_id="start")
    
    with TaskGroup("Level1", tooltip="Level1 Tasks") as level1:
        task1_l1 = PythonOperator(
            task_id="task1",
            python_callable=lambda: task1_func("Level1")
        )
        task2_l1 = PythonOperator(
            task_id="task2",
            python_callable=lambda: task2_func("Level1")
        )
        task1_l1 >> task2_l1

    with TaskGroup("Level2", tooltip="Level2 Tasks") as level2:
        task1_l2 = PythonOperator(
            task_id="task1",
            python_callable=lambda: task1_func("Level2")
        )
        task2_l2 = PythonOperator(
            task_id="task2",
            python_callable=lambda: task2_func("Level2")
        )
        task1_l2 >> task2_l2

    with TaskGroup("Level3", tooltip="Level3 Tasks") as level3:
        task1_l3 = PythonOperator(
            task_id="task1",
            python_callable=lambda: task1_func("Level3")
        )
        task2_l3 = PythonOperator(
            task_id="task2",
            python_callable=lambda: task2_func("Level3")
        )
        task1_l3 >> task2_l3

    with TaskGroup("Level4", tooltip="Level4 Tasks") as level4:
        task1_l4 = PythonOperator(
            task_id="task1",
            python_callable=lambda: task1_func("Level4")
        )
        task2_l4 = PythonOperator(
            task_id="task2",
            python_callable=lambda: task2_func("Level4")
        )
        task1_l4 >> task2_l4

    end = DummyOperator(task_id="end")
    
    start >> level1 >> level2 >> level3 >> level4 >> end
