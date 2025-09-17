from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="my_first_dag",
    default_args={"start_date": datetime(2024, 12, 1)},
    schedule_interval="@daily",
    catchup=False,
) as dag:
    start = DummyOperator(task_id="start")

    bash_task = BashOperator(
        task_id='bash_task',
        bash_command='echo "bash"'
    )

    def python_print():
        print("python")

    python_task = PythonOperator(
        task_id='python_task',
        python_callable=python_print
    )

    end = DummyOperator(task_id="end")

    start >> bash_task >> python_task >> end