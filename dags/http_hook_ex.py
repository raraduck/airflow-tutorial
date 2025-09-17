from airflow import DAG
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 12, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

def fetch_and_store_stock_data():
    http_hook = HttpHook(method="GET", http_conn_id="alpha-vantage-conn")
    params = {
        "function": "TIME_SERIES_INTRADAY",
        "symbol": "AAPL",
        "interval": "1min",
        "apikey": ""  
    }
    response = http_hook.run(endpoint="query", data=params)
    data = response.json()
    time_series = data.get("Time Series (1min)")
    if not time_series:
        raise Exception("No time series data returned")
    latest_timestamp = sorted(time_series.keys())[-1]
    latest_data = time_series[latest_timestamp]
    latest_price = latest_data.get("1. open")
    pg_hook = PostgresHook(postgres_conn_id="postgres-conn")
    sql = "INSERT INTO aapl_stock (date, price) VALUES (%s, %s)"
    pg_hook.run(sql, parameters=(latest_timestamp, latest_price))

dag = DAG(
    "stock_pipeline",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False
)

store_stock_data = PythonOperator(
    task_id="fetch_and_store_stock_data",
    python_callable=fetch_and_store_stock_data,
    dag=dag
)