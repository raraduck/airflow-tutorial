from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import random

def error_task():
    if random.choice([True, False]):
        raise Exception("Error occurred")
    print("Task succeeded")

def failure_callback(context):
    print("Failure callback triggered")

with DAG(
    "error_handling_example", 
    start_date=days_ago(1), 
    schedule_interval=None, 
    catchup=False
) as dag:
    task = PythonOperator(
        task_id="error_task",
        python_callable=error_task,
        retries=2,
        retry_delay=timedelta(seconds=10),
        on_failure_callback=failure_callback
    )