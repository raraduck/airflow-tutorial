from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.operators.dummy import DummyOperator
from datetime import datetime

with DAG("taskgroup_example", 
         start_date=datetime(2024, 12, 1), 
         schedule_interval='@daily',
         catchup=False
) as dag:
    start = DummyOperator(task_id="start")

    with TaskGroup("group1") as group1:
        t1 = DummyOperator(task_id="task1")
        t2 = DummyOperator(task_id="task2")
        t1 >> t2  

    end = DummyOperator(task_id="end")
    start >> group1 >> end
