from airflow import DAG
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime

default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 12, 1),
    "retries": 0
}

dag = DAG(
    "example_slack_webhook_task",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False
)

start = DummyOperator(task_id="start", dag=dag)

notify = SlackWebhookOperator(
    task_id="slack_notification",
    slack_webhook_conn_id="slack-http",  
    message="Airflow DAG 실행 완료!",
    channel="#airflow-noti",
    dag=dag
)

start >> notify