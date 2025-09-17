from airflow import DAG
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime
import random

# DAG 정의
dag = DAG(
    'branching_example',
    description='Branch and Trigger',
    schedule_interval='0 9 * * *',
    start_date=datetime(2024, 12, 1),
    catchup=False
)

def generate_random_number():
    number = random.randint(1, 10)
    print(f"Generated Number: {number}")

def branch_task():
    number = random.randint(1, 10) 
    print(f"Branching Decision Number: {number}")
    
    if number >= 5:
        return 'high_task'
    else:
        return 'low_task'

def high_task():
    print("High Task Executed: Number is 5 or more!")

def low_task():
    print("Low Task Executed: Number is less than 5!")

def final_task():
    print("Final Task Executed: Always runs regardless of previous results.")


generate_number = PythonOperator(
    task_id='generate_number',
    python_callable=generate_random_number,
    dag=dag
)

branch = BranchPythonOperator(
    task_id='branch',
    python_callable=branch_task,
    dag=dag
)

high_task = PythonOperator(
    task_id='high_task',
    python_callable=high_task,
    dag=dag
)

low_task = PythonOperator(
    task_id='low_task',
    python_callable=low_task,
    dag=dag
)

final_task = PythonOperator(
    task_id='final_task',
    python_callable=final_task,
    trigger_rule='all_done',
    dag=dag
)

generate_number >> branch
branch >> [high_task, low_task]
[high_task, low_task] >> final_task
