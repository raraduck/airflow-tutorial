import random
import time
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator

def unreliable_task_with_backoff(max_attempts=3):
    attempt = 1
    while attempt <= max_attempts:
        try:
            if random.random() < 0.7:
                raise Exception("Simulated failure")
            print(f"Task succeeded on attempt {attempt}")
            return "Success"
        except Exception as e:
            print(f"Attempt {attempt} failed: {e}")
            if attempt == max_attempts:
                raise e
            sleep_time = 2 ** attempt
            print(f"Sleeping for {sleep_time} seconds before retrying...")
            time.sleep(sleep_time)
            attempt += 1

def decide_branch():
    try:
        unreliable_task_with_backoff()
        return "success_path"
    except Exception as e:
        print("Fallback path triggered due to failure:", e)
        return "fallback_path"

def fallback_task():
    print("Executing fallback procedure")
    return "Fallback executed"

default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 12, 1),
    "retries": 0,
}

dag = DAG(
    "exponential_backoff_fallback",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False
)

branch = BranchPythonOperator(
    task_id="branch_decision",
    python_callable=decide_branch,
    dag=dag
)

success_path = DummyOperator(
    task_id="success_path",
    dag=dag
)

fallback = PythonOperator(
    task_id="fallback_path",
    python_callable=fallback_task,
    dag=dag
)

end = DummyOperator(
    task_id="end",
    dag=dag
)

branch >> success_path >> end
branch >> fallback >> end
