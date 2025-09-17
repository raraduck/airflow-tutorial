from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator
from datetime import datetime

# DAG 정의
with DAG(
    dag_id="daily_task_pipeline",
    default_args={"start_date": datetime(2024, 12, 1)},
    schedule_interval="0 9 * * *",
    catchup=False,
) as dag:
    start = DummyOperator(task_id="start")
    
    task_A = BashOperator(
        task_id="task_A",
        bash_command="echo 'Task A is running!'",
    )
    
    task_B1 = BashOperator(
        task_id="task_B1",
        bash_command="echo 'Task B1 is running!'",
    )
    
    task_B2 = BashOperator(
        task_id="task_B2",
        bash_command="echo 'Task B2 is running!'",
    )
    
    task_C = BashOperator(
        task_id="task_C",
        bash_command="echo 'Task C is running after B1 and B2!'",
    )
    
    end = DummyOperator(task_id="end")
    
    # Task Dependency 설정
    start >> task_A >> [task_B1, task_B2] >> task_C >> end