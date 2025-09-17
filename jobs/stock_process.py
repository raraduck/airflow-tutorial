import os
import sys
import shutil
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, lag, unix_timestamp
from pyspark.sql.window import Window
from pyspark.sql.types import TimestampType

def main():
    if len(sys.argv) != 3:
        print("Usage: stock_process.py <input_csv> <output_csv>")
        sys.exit(-1)

    input_csv = sys.argv[1]
    output_csv = sys.argv[2]

    spark = SparkSession.builder.appName("StockAnalysis") \
                .config("spark.executor.memory", "512m") \
                .config("spark.driver.memory", "512m") \
                .config("spark.task.cpus", "1") \
                .config("spark.dynamicAllocation.enabled", "false") \
                .getOrCreate()

    df = spark.read.option("header", "true").option("inferSchema", "true").csv(input_csv)
    df = df.withColumn("Date", col("Date").cast(TimestampType())) \
           .withColumn("Date", unix_timestamp("Date"))
    df = df.orderBy("Date")

    # 5-day moving average calculation 
    window_spec = Window.orderBy("Date").rowsBetween(-4, 0)

    # 5-day moving average of Close price
    df = df.withColumn("moving_avg_5", avg("Close").over(window_spec))

    # daily return: (Close / previous Close - 1)
    window_spec_lag = Window.orderBy("Date")
    df = df.withColumn("prev_close", lag("Close").over(window_spec_lag))
    df = df.withColumn("daily_return", (col("Close") / col("prev_close") - 1))

    df_final = df.coalesce(1)
    df_final = df_final.toPandas()
    df_final.to_csv(output_csv, mode='w', header=True)

    spark.stop()

if __name__ == "__main__":
    main()
