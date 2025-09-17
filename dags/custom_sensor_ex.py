from airflow import DAG
from airflow.operators.dummy import DummyOperator
from datetime import datetime, timedelta
from file_count_sensor import FileCountSensor

default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 12, 1),
    "retries": 0,
}

dag = DAG(
    "file_count_sensor_example",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False
)

sensor_task = FileCountSensor(
    task_id="check_csv_files",
    directory="/var/lib/airflow/data",
    file_regex=r".*\.csv$",
    min_count=3,
    poke_interval=10,
    timeout=600,
    dag=dag
)

end = DummyOperator(task_id="end", dag=dag)

sensor_task >> end
