from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

def push_data(**kwargs):
    kwargs['ti'].xcom_push(key='message', value='Hello from Task A')

def pull_data(**kwargs):
    message = kwargs['ti'].xcom_pull(task_ids='push_task', key='message')
    print(f"Received message: {message}")

dag = DAG(
    'xcom_example', 
    start_date=datetime(2024, 12, 1), 
    schedule_interval='@daily',
    catchup=False
    )

push_task = PythonOperator(
    task_id='push_task', 
    python_callable=push_data, 
    provide_context=True, 
    dag=dag)

pull_task = PythonOperator(
    task_id='pull_task', 
    python_callable=pull_data, 
    provide_context=True, 
    dag=dag)

end_task = PythonOperator(
    task_id='end_task', 
    python_callable=lambda: print("All tasks completed"), 
    dag=dag)

push_task >> pull_task >> end_task