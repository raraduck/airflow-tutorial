from airflow import DAG
from airflow.operators.dummy import DummyOperator
from datetime import datetime

with DAG(
    "dagA", 
    start_date=datetime(2024, 12, 1), 
    schedule_interval='*/1 * * * *',
    catchup=False
) as dag:
    task_A = DummyOperator(task_id="task_A")