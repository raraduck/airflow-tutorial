from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

import pandas as pd
import yfinance as yf

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 12, 1),
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG('data_pipeline_example', default_args=default_args, 
            schedule_interval='@daily', catchup=False)

def download_data():
    ticker = yf.Ticker('AAPL')
    data = ticker.history(period="100d", interval="1d", auto_adjust=True)
    print(data)
    data.to_csv("/var/lib/airflow/tmp/aapl_data.csv")

def process_data():
    df = pd.read_csv("/var/lib/airflow/tmp/aapl_data.csv", header='infer')
    print(df.head())
    df['MA_10'] = df['Close'].rolling(window=10).mean()
    df.to_csv("/var/lib/airflow/tmp/aapl_data_processed.csv", index=False)

def save_data():
    print("Processed data saved to /var/lib/airflow/tmp/aapl_data_processed.csv")

download_task = PythonOperator(task_id='download_data', python_callable=download_data, dag=dag)
process_task = PythonOperator(task_id='process_data', python_callable=process_data, dag=dag)
save_task = PythonOperator(task_id='save_data', python_callable=save_data, dag=dag)

download_task >> process_task >> save_task
