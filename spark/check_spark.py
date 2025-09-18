import argparse, os
from pyspark.sql import SparkSession
# import findspark
from dotenv import load_dotenv  # ✅ 추가
load_dotenv(dotenv_path="/var/lib/airflow/.env")  # 필요시 절대경로 지정

# os.environ["JAVA_HOME"] = os.getenv("JAVA_HOME", "/usr/lib/jvm/java-1.8.0-amazon-corretto.x86_64/jre")
# os.environ["SPARK_HOME"] = os.getenv("SPARK_HOME", "/var/lib/airflow/spark/spark-3.4.1-bin-hadoop3")
# os.environ["PATH"] = f'{os.environ["SPARK_HOME"]}/bin:{os.environ["SPARK_HOME"]}/sbin:' + os.environ["PATH"]
# findspark.init(os.environ["SPARK_HOME"])

def main(access_key: str=None, secret_key: str=None):
    spark = SparkSession.builder \
        .appName("first-spark-app") \
        .master("local[*]") \
        .config("spark.jars", "/var/lib/airflow/spark/jars/hadoop-aws-3.3.4.jar,/var/lib/airflow/spark/jars/aws-java-sdk-bundle-1.12.262.jar") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.access.key", access_key) \
        .config("spark.hadoop.fs.s3a.secret.key", secret_key) \
        .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
        .getOrCreate()

    # print(spark)
    # df = spark.range(10)
    # df.show()

    df = spark.read.text("s3a://databricks-workspace-stack-60801-bucket/users_orders/users.csv")

    # 처음 몇 줄 출력
    df.show(10, truncate=False)

    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Spark ETL Job")
    # parser.add_argument("--users", required=True, help="Path to users CSV file in S3")
    # parser.add_argument("--orders", required=True, help="Path to orders JSON file in S3")
    parser.add_argument("--access-key", required=True, help="AWS Access Key")
    parser.add_argument("--secret-key", required=True, help="AWS Secret Key")

    args = parser.parse_args()
    print(args.access_key, args.secret_key)
    # main(args.access_key, args.secret_key)
