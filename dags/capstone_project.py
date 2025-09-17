from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.task_group import TaskGroup
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import yfinance as yf
import os

with open("/var/lib/airflow/stock_list.txt", "r") as f:
    STOCKS = [line.strip() for line in f if line.strip()]

def download_stock_data(symbol):
    ticker = yf.Ticker(symbol)
    data = ticker.history(period="5d", interval="1d", auto_adjust=True)
    filepath = f"/var/lib/airflow/tmp/{symbol}.csv"
    data.to_csv(filepath)
    print(f"{symbol} data downloaded to {filepath}")

def merge_all_results():
    import pandas as pd
    merged_df = pd.DataFrame()
    for symbol in STOCKS:
        file_spark = f"/var/lib/airflow/tmp/{symbol}_processed.csv"
        if os.path.exists(file_spark):
            df = pd.read_csv(file_spark)
            df["symbol"] = symbol
            merged_df = pd.concat([merged_df, df])
    merged_filepath = "/var/lib/airflow/tmp/merged_data.csv"
    merged_df.to_csv(merged_filepath, index=False)
    print(f"Merged results saved to {merged_filepath}")
    pg_hook = PostgresHook(postgres_conn_id="postgres-conn")
    table_exists = pg_hook.get_first("SELECT to_regclass('public.stock_data')")
    if table_exists is None or table_exists[0] is None:
        create_sql = """
        CREATE TABLE IF NOT EXISTS stock_data (
            symbol TEXT,
            date TIMESTAMP,
            open NUMERIC,
            high NUMERIC,
            low NUMERIC,
            close NUMERIC,
            volume BIGINT,
            moving_avg_5 NUMERIC
        );
        """
        pg_hook.run(create_sql)
        
    with pg_hook.get_conn() as conn:
        with conn.cursor() as cur:
            for _, row in merged_df.iterrows():
                cur.execute(
                    "INSERT INTO stock_data (symbol, date, open, high, low, close, volume, moving_avg_5) VALUES (%s, to_timestamp(%s), %s, %s, %s, %s, %s, %s)",
                    (row.get("symbol"), row.get("Date"), row.get("Open"), row.get("High"), row.get("Low"), row.get("Close"), row.get("Volume"), row.get("moving_avg_5"))
                )
        conn.commit()

default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 12, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

dag = DAG(
    "capstone_stock_pipeline",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False
)

with dag:
    start = DummyOperator(task_id="start")
    
    with TaskGroup("data_ingestion", tooltip="Download stock data") as ingestion:
        for symbol in STOCKS:
            PythonOperator(
                task_id=f"download_{symbol}",
                python_callable=download_stock_data,
                op_args=[symbol]
            )
    
    with TaskGroup("spark_processing", tooltip="Process data with Spark") as processing:
        for symbol in STOCKS:
            SparkSubmitOperator(
                task_id=f"{symbol}_spark",
                application="/var/lib/airflow/jobs/stock_process.py",
                conn_id="spark-conn",
                application_args=[f"/var/lib/airflow/tmp/{symbol}.csv", f"/var/lib/airflow/tmp/{symbol}_processed.csv"],
                executor_cores=2
            )
    
    merge = PythonOperator(
        task_id="merge_results",
        python_callable=merge_all_results
    )
    
    end = DummyOperator(task_id="end")
    
    start >> ingestion >> processing >> merge >> end

