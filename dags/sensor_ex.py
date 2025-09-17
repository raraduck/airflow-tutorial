from airflow import DAG
from airflow.sensors.time_sensor import TimeSensor
from airflow.sensors.filesystem import FileSensor
from datetime import datetime, time

with DAG("sensor_combined_example", start_date=datetime(2024, 12, 1), schedule_interval=None) as dag:
    wait_until_time = TimeSensor(
        task_id="wait_until_time",
        target_time=time(0, 1, 0)
    )
    
    wait_for_file = FileSensor(
        task_id="wait_for_file",
        filepath="/var/lib/airflow/data_ready.txt",
        mode="reschedule"
    )
    
    wait_until_time >> wait_for_file