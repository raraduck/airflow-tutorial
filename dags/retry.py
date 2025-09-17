from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import random


dag = DAG(
    'retry_example_dag',
    description='DAG with retry mechanism',
    schedule_interval='@daily', 
    start_date=datetime(2024, 12, 1),
    catchup=False
)

def unreliable_task():
    if random.choice([True, False]):  
        raise ValueError("Task failed! Retrying...")
    print("Task succeeded!")

retry_task = PythonOperator(
    task_id='retry_task',
    python_callable=unreliable_task,
    retries=3,  
    retry_delay=timedelta(minutes=5), 
    dag=dag
)

retry_task
