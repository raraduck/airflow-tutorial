from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator
from datetime import datetime

default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 1, 1),
}

with DAG(
    "databricks_notebook_example", 
    default_args=default_args, 
    schedule_interval=None
    ) as dag:

    notebook_task = DatabricksSubmitRunOperator(
        task_id="run_databricks_notebook",
        databricks_conn_id="databricks_default",
        json={
            "existing_cluster_id": "0918-132607-3d0hh606",
            "notebook_task": {
                "notebook_path": "/Workspace/Users/dwngcp2506@gmail.com/notebook01.ipynb",  # 워크스페이스 경로
                # "base_parameters": {
                #     "input": "dbfs:/mnt/input",
                #     "output": "dbfs:/mnt/output"
                # }
            }
        },
    )