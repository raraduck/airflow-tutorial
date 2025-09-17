from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.subdag import SubDagOperator
from datetime import datetime

def subdag(parent_dag, child_dag, args):
    with DAG(f"{parent_dag}.{child_dag}", default_args=args) as dag:
        t1 = DummyOperator(task_id="sub_task1")
        t2 = DummyOperator(task_id="sub_task2")
        t1 >> t2
    return dag

with DAG("subdag_example", 
         start_date=datetime(2024, 12, 1), 
         schedule_interval='@daily',
         catchup=False
) as dag:
    start = DummyOperator(task_id="start")
    
    sub_dag = SubDagOperator(
        task_id="sub_dag",
        subdag=subdag("subdag_example", "sub_dag", {}),
    )

    end = DummyOperator(task_id="end")
    start >> sub_dag >> end
