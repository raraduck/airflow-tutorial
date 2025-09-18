from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime
from pyspark.sql import SparkSession
import os, shutil, pyspark

with DAG(
    dag_id="my_first_spark_dag",
    default_args={"start_date": datetime(2024, 12, 1)},
    schedule_interval="@daily",
    catchup=False,
) as dag:
    start = DummyOperator(task_id="start")

    bash_task = BashOperator(
        task_id='bash_task',
        bash_command='echo "bash"'
    )

    def python_print():
        print("python")

    def check_spark():
        os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-1.8.0-amazon-corretto.x86_64/jre"
        os.environ["SPARK_HOME"] = "/var/lib/airflow/spark/spark-3.4.1-bin-hadoop3"
        os.environ["PATH"]       = f'{os.environ["SPARK_HOME"]}/bin:{os.environ["SPARK_HOME"]}/sbin:' + os.environ["PATH"]
        spark = SparkSession.builder \
            .appName("first-spark-app") \
            .master("local[*]") \
            .getOrCreate()

        print(spark)
        df = spark.range(10)
        df.show()

    python_task = PythonOperator(
        task_id='python_task',
        python_callable=check_spark
    )

    end = DummyOperator(task_id="end")

    start >> bash_task >> python_task >> end