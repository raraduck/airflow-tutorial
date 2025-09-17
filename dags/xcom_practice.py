from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def push_data(**kwargs):
    return [1, 2, 3, 4, 5]

def pull_and_process_data(**kwargs):
    ti = kwargs['ti']
    numbers = ti.xcom_pull(task_ids='push_task')
    processed = [x + 10 for x in numbers]
    print("Processed Data:", processed)

dag = DAG(
    'xcom_exercise', 
    start_date=datetime(2024, 12, 1), 
    schedule_interval='@daily',
    catchup=False)

push_task = PythonOperator(
    task_id='push_task', 
    python_callable=push_data, 
    dag=dag)

pull_task = PythonOperator(
    task_id='pull_task', 
    python_callable=pull_and_process_data, 
    dag=dag)

end_task = PythonOperator(
    task_id='end_task', 
    python_callable=lambda: print("All tasks completed"), 
    dag=dag)

push_task >> pull_task >> end_task