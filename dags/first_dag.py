from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator, PythonVirtualenvOperator
from airflow.operators.bash import BashOperator
# from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

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

def python_print():
    print("python")

def check_spark(access_key: str=None, secret_key: str=None):
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F
    from pyspark.sql.types import (
        StructType, StructField, StringType, IntegerType, DoubleType, DateType
    )
    spark = SparkSession.builder \
        .appName("first-spark-app") \
        .master("local[*]") \
        .config("spark.jars", "/var/lib/airflow/spark/jars/hadoop-aws-3.3.4.jar,/var/lib/airflow/spark/jars/aws-java-sdk-bundle-1.12.262.jar") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID")) \
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY")) \
        .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
        .getOrCreate()

    users_path = "s3a://databricks-workspace-stack-60801-bucket/users_orders/users.csv"
    orders_path = "s3a://databricks-workspace-stack-60801-bucket/users_orders/orders.json"
    # ---------------------------
    # 1. 스키마 정의
    # ---------------------------
    users_schema = StructType([
        StructField("user_id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("gender", StringType(), True),
    ])

    orders_schema = StructType([
        StructField("order_id", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField("order_ts", StringType(), True),  # String → Date 변환 예정
        StructField("amount", DoubleType(), True),
    ])

    # ---------------------------
    # 2. DataFrame 로드
    # ---------------------------
    users_df = spark.read.csv(users_path, header=True, schema=users_schema)
    users_df.show(10, truncate=False)

    raw = spark.read.text(orders_path)
    
    # RTF/제어문자 제거 + 역슬래시 제거
    # json_lines = raw.rdd.map(lambda row: row[0]) \
    #     .map(lambda line: line.strip()) \
    #     .filter(lambda line: line.startswith("{") or line.startswith("[")) \
    #     .map(lambda line: line.replace("\\", ""))  # \{ → { 로 정리

    json_lines = raw.rdd.map(lambda row: row[0]) \
        .map(lambda line: line.strip()) \
        .filter(lambda line: line.startswith("{") or line.startswith("[") or line.startswith("\\{")) \
        .map(lambda line: line.replace("\\", "")) \
        .filter(lambda line: line.startswith("{") or line.startswith("["))

    # Spark에 JSON으로 다시 읽기
    # orders_df = spark.read.json(json_lines, schema=orders_schema)
    orders_df = spark.read.json(json_lines, schema=orders_schema, multiLine=True)
    # 모든 컬럼이 null 인 row 제거
    orders_df = orders_df.dropna(how="all")
    orders_df.show(20, truncate=False)

    # ---------------------------
    # 3. 컬럼 추가/변환
    # ---------------------------
    users_df = users_df.withColumn("signup_year", F.lit(2023).cast(IntegerType()))
    orders_df = orders_df.withColumn("order_date", F.to_date("order_ts"))

    # ---------------------------
    # 4. Join
    # ---------------------------
    joined_df = users_df.join(orders_df, on="user_id", how="inner") \
        .select(
            "order_id", "user_id", "name", "gender",
            "signup_year", "order_date", "amount"
        )

    # 처음 몇 줄 출력
    joined_df.show(10, truncate=False)
    spark.stop()

def process_data(access_key, secret_key, users_path=None, orders_path=None):
    from pyspark.sql import SparkSession
    print("Access Key:", access_key)
    print("Secret Key:", secret_key)
    if users_path:
        print("Users file:", users_path)
    if orders_path:
        print("Orders file:", orders_path)

    spark = SparkSession.builder \
        .appName("spark-app") \
        .master("local[*]") \
        .config("spark.jars", "/var/lib/airflow/spark/jars/hadoop-aws-3.3.4.jar,/var/lib/airflow/spark/jars/aws-java-sdk-bundle-1.12.262.jar") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.access.key", access_key) \
        .config("spark.hadoop.fs.s3a.secret.key", secret_key) \
        .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
        .getOrCreate()

    df1 = spark.read.text(users_path)
    df1.show(10, truncate=False)
    df2 = spark.read.text(orders_path)
    df2.show(10, truncate=False)
    spark.stop()

def hw5_8(access_key, secret_key, users_path=None, orders_path=None):
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F
    from pyspark.sql.types import (
        StructType, StructField, StringType, IntegerType, DoubleType, DateType
    )

    spark = SparkSession.builder \
        .appName("spark-app") \
        .master("local[*]") \
        .config("spark.jars", "/var/lib/airflow/spark/jars/hadoop-aws-3.3.4.jar,/var/lib/airflow/spark/jars/aws-java-sdk-bundle-1.12.262.jar") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.access.key", access_key) \
        .config("spark.hadoop.fs.s3a.secret.key", secret_key) \
        .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
        .getOrCreate()

    # ---------------------------
    # 1. 스키마 정의
    # ---------------------------
    users_schema = StructType([
        StructField("user_id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("gender", StringType(), True),
    ])

    orders_schema = StructType([
        StructField("order_id", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField("order_ts", StringType(), True),  # String → Date 변환 예정
        StructField("amount", DoubleType(), True),
    ])

    # ---------------------------
    # 2. DataFrame 로드
    # ---------------------------
    users_df = spark.read.csv(users_path, header=True, schema=users_schema)
    orders_df = spark.read.json(orders_path, schema=orders_schema)

    # ---------------------------
    # 3. 컬럼 추가/변환
    # ---------------------------
    users_df = users_df.withColumn("signup_year", F.lit(2023).cast(IntegerType()))
    orders_df = orders_df.withColumn("order_date", F.to_date("order_ts"))

    # ---------------------------
    # 4. Join
    # ---------------------------
    joined_df = users_df.join(orders_df, on="user_id", how="inner") \
        .select(
            "order_id", "user_id", "name", "gender",
            "signup_year", "order_date", "amount"
        )

    # ---------------------------
    # 5. Dataset API 타입 안정성
    # ---------------------------
    # (PySpark에서는 TypedDict 대체로 schema 기반 처리)
    from typing import TypedDict

    class JoinedSchema(TypedDict):
        order_id: str
        user_id: str
        name: str
        gender: str
        signup_year: int
        order_date: str
        amount: float

    # Dataset 흉내: schema 기반 DataFrame
    joined_df.printSchema()

    # ---------------------------
    # 6. amount >= 100 필터링
    # ---------------------------
    filtered_df = joined_df.filter(F.col("amount") >= 100)

    print("=== amount >= 100 주문만 출력 ===")
    filtered_df.show(truncate=False)

    # ---------------------------
    # 7. UDF 정의 및 적용
    # ---------------------------
    def segment_by_year(year: int) -> str:
        if year < 2020:
            return "OLD"
        elif 2020 <= year <= 2022:
            return "MID"
        else:
            return "NEW"

    from pyspark.sql.functions import udf
    segment_udf = udf(segment_by_year, StringType())

    final_df = filtered_df.withColumn("user_segment", segment_udf("signup_year"))

    print("=== UDF 적용 결과 ===")
    final_df.show(truncate=False)

    # ---------------------------
    # 8. Spark SQL로 비교
    # ---------------------------
    final_df.createOrReplaceTempView("joined_table")

    sql_result = spark.sql("""
        SELECT order_id, user_id, name, gender, signup_year, order_date, amount,
               CASE
                   WHEN signup_year < 2020 THEN 'OLD'
                   WHEN signup_year BETWEEN 2020 AND 2022 THEN 'MID'
                   ELSE 'NEW'
               END AS user_segment
        FROM joined_table
        WHERE amount >= 100
    """)

    print("=== SQL 결과 ===")
    sql_result.show(truncate=False)

    spark.stop()

with DAG(
    dag_id="virtualenv-spark-app",
    start_date=datetime(2025, 9, 1),
    schedule_interval="0 6 * * *",  # 매일 오전 6시
    catchup=False,
    # default_args={"start_date": datetime(2024, 12, 1)},
    # schedule_interval="@daily",
    # catchup=False,
    # tags=["spark", "batch"],
) as dag:
    start = DummyOperator(task_id="start")

    bash_task = BashOperator(
        task_id='bash_task',
        bash_command='echo "bash"'
    )

    python_task = PythonOperator(
        task_id='python_task',
        python_callable=python_print
    )
    
    spark_task = PythonOperator(
        task_id='spark_task',
        python_callable=check_spark
    )

    virtualenv_task = PythonVirtualenvOperator(
        task_id="virtualenv_task",
        python_callable=hw5_8,
        requirements=[
            # "pandas==2.2.2", 
            # "scikit-learn==1.5.1",
            "pyspark==3.4.1",
            # "boto3",
            ],
        system_site_packages=False,
        op_args=[
            os.getenv("AWS_ACCESS_KEY_ID"),
            os.getenv("AWS_SECRET_ACCESS_KEY"),
        ],
        op_kwargs={
            "users_path": "s3a://databricks-workspace-stack-60801-bucket/users_orders/users.csv",
            "orders_path": "s3a://databricks-workspace-stack-60801-bucket/users_orders/orders.json",
        },
    )

    end = DummyOperator(task_id="end")

    start >> virtualenv_task >> end

# =====================
# init main 부분 추가
# =====================
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run Spark job with AWS credentials")
    parser.add_argument("--access-key", required=True, help="AWS Access Key")
    parser.add_argument("--secret-key", required=True, help="AWS Secret Key")

    args = parser.parse_args()
    check_spark(args.access_key, args.secret_key)