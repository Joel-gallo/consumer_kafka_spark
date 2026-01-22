import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, MapType

from env_variables import KAFKA_BROKER, KAFKA_TOPIC, SPARK_MASTER, WAREHOUSE_PATH

def spark_kafka_consuming():
    spark = (
        SparkSession
        .builder
        .appName("user_events_batch")
        .master("local[2]")
        .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1"
        )
        .getOrCreate()
    )

    df = (
        spark.read
        .format("kafka")
        .option("kafka.bootstrap.servers", ",".join(KAFKA_BROKER))
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "earliest")
        .load()
    )

    df_parsed = df.select(
        col("key").cast("string"),
        col("value").cast("string"),
        col("topic"),
        col("partition"),
        col("offset"),
        col("timestamp")
    )


    df_parsed.write.mode("append").parquet(WAREHOUSE_PATH)

    spark.stop()
