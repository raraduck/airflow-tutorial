from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime
import os, shutil
import findspark
import argparse
from dotenv import load_dotenv  # ✅ 추가
load_dotenv(dotenv_path="/var/lib/airflow/.env")  # 필요시 절대경로 지정

# os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-1.8.0-amazon-corretto.x86_64/jre"
# os.environ["SPARK_HOME"] = "/var/lib/airflow/spark/spark-3.4.1-bin-hadoop3"
# os.environ["PATH"]       = f'{os.environ["SPARK_HOME"]}/bin:{os.environ["SPARK_HOME"]}/sbin:' + os.environ["PATH"]

os.environ["JAVA_HOME"] = os.getenv("JAVA_HOME", "/usr/lib/jvm/java-1.8.0-amazon-corretto.x86_64/jre")
os.environ["SPARK_HOME"] = os.getenv("SPARK_HOME", "/var/lib/airflow/spark/spark-3.4.1-bin-hadoop3")
os.environ["PATH"] = f'{os.environ["SPARK_HOME"]}/bin:{os.environ["SPARK_HOME"]}/sbin:' + os.environ["PATH"]
findspark.init(os.environ["SPARK_HOME"])


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

    def check_spark(access_key: str=None, secret_key: str=None):
        from pyspark.sql import SparkSession
        spark = SparkSession.builder \
            .appName("first-spark-app") \
            .master("local[*]") \
            .config("spark.jars", "/var/lib/airflow/spark/jars/hadoop-aws-3.3.4.jar,/var/lib/airflow/spark/jars/aws-java-sdk-bundle-1.12.262.jar") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.hadoop.fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID")) \
            .config("spark.hadoop.fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY")) \
            .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
            .getOrCreate()

        # print(spark)
        # df = spark.range(10)
        # df.show()

        df = spark.read.text("s3a://databricks-workspace-stack-60801-bucket/users_orders/users.csv")

        # 처음 몇 줄 출력
        df.show(10, truncate=False)

    python_task = PythonOperator(
        task_id='python_task',
        python_callable=python_print
    )
    
    spark_task = PythonOperator(
        task_id='spark_task',
        python_callable=check_spark
    )

    end = DummyOperator(task_id="end")

    start >> bash_task >> python_task >> spark_task >> end

# =====================
# init main 부분 추가
# =====================
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run Spark job with AWS credentials")
    parser.add_argument("--access-key", required=True, help="AWS Access Key")
    parser.add_argument("--secret-key", required=True, help="AWS Secret Key")

    args = parser.parse_args()
    check_spark(args.access_key, args.secret_key)