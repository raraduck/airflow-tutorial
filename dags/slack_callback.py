import random
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator

def random_failure_task():
    if random.choice([True, False]):
        raise Exception("Random failure occurred!")
    print("Task succeeded")

def slack_failure_callback(context):
    message = f"Task {context.get('task_instance').task_id} failed at {context.get('execution_date')}"
    slack_alert = SlackWebhookOperator(
        task_id="slack_failure",
        slack_webhook_conn_id="slack-http",
        message=message,
        channel="#airflow-noti"
    )
    slack_alert.execute(context=context)

default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 12, 1),
    "retries": 0,
}

dag = DAG(
    "slack_callback",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False
)

failure_task = PythonOperator(
    task_id="failure_task",
    python_callable=random_failure_task,
    on_failure_callback=slack_failure_callback,
    dag=dag
)

end = DummyOperator(task_id="end", dag=dag)

failure_task >> end