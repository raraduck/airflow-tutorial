from airflow import DAG
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.operators.dummy import DummyOperator
from datetime import datetime, timedelta

with DAG(
    "dagB", 
    start_date=datetime(2024, 12, 1), 
    schedule_interval='*/1 * * * *',
    catchup=False
) as dag:
    wait_for_A = ExternalTaskSensor(
        task_id="wait_for_A",
        external_dag_id="dagA",
        external_task_id="task_A",
        execution_delta=timedelta(minutes=1),
        poke_interval=10,
        timeout=120
    )
    
    task_B = DummyOperator(task_id="task_B")
    
    wait_for_A >> task_B
