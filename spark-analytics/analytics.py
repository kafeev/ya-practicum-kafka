#!/usr/bin/env python3
import os
import json
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, to_json, struct, lit,
    count, desc,
)
from pyspark.sql.types import StructType, StructField, StringType

CERTS_DIR = "/opt/bitnami/spark/ssl/certs"
HDFS_RAW_PATH = "hdfs://hdfs-namenode:8020/user/spark/raw"
RECOMMENDATIONS_TOPIC = "recommendations"


def create_spark_session():
    """Создание Spark сессии с SSL и HDFS настройками."""
    spark = SparkSession.builder \
        .appName("AnalyticsSystem") \
        .config("spark.sql.streaming.checkpointLocation", "/tmp/checkpoint-analytics") \
        .config("spark.hadoop.fs.defaultFS", "hdfs://hdfs-namenode:8020") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def get_kafka_config():
    """SSL-конфигурация Kafka для Spark (JKS)."""
    return {
        "kafka.bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP", "backup1:9195,backup2:9196,backup3:9197"),
        "kafka.security.protocol": "SSL",
        "kafka.ssl.truststore.location": f"{CERTS_DIR}/kafka.truststore.jks",
        "kafka.ssl.truststore.password": "kafka123",
        "kafka.ssl.keystore.location": f"{CERTS_DIR}/client.analytics.keystore.jks",
        "kafka.ssl.keystore.password": "kafka123",
        "kafka.ssl.key.password": "kafka123",
        "kafka.ssl.endpoint.identification.algorithm": "",
    }


def write_batch(batch_df, batch_id, kafka_config):
    """Обработка микро-батча: landing в HDFS + запись рекомендаций в Kafka."""
    # 1) Перенос сырых данных в HDFS (требование базового варианта, best-effort)
    try:
        batch_df.write.mode("append").json(HDFS_RAW_PATH)
        print(f"[batch {batch_id}] raw data landed to HDFS ({HDFS_RAW_PATH})")
    except Exception as e:
        print(f"[batch {batch_id}] HDFS landing skipped: {e}")

    # 2) Аналитика: топ популярных поисковых запросов в этом батче
    searches = batch_df.filter(col("type") == "search").filter(col("query").isNotNull())
    if searches.rdd.isEmpty():
        print(f"[batch {batch_id}] no search events, skip")
        return

    top = (searches
           .groupBy("query")
           .agg(count("*").alias("popularity"))
           .orderBy(desc("popularity"))
           .limit(10))

    recommendations = top.select(to_json(struct(
        lit("popular_search").alias("type"),
        col("query").alias("recommendation"),
        col("popularity"),
        lit(datetime.now().isoformat()).alias("generated_at"),
    )).alias("value"))

    try:
        (recommendations.write
         .format("kafka")
         .options(**kafka_config)
         .option("topic", RECOMMENDATIONS_TOPIC)
         .save())
        print(f"[batch {batch_id}] recommendations written to topic '{RECOMMENDATIONS_TOPIC}'")
    except Exception as e:
        print(f"[batch {batch_id}] failed to write recommendations: {e}")


def main():
    print("Starting Analytics System...")
    spark = create_spark_session()
    kafka_config = get_kafka_config()

    # Реальный формат сообщений в client_requests (см. client_api.py):
    #   {"type": "search", "user_id": ..., "query": ..., "ts": ...}
    #   {"type": "recommend", "user_id": ..., "ts": ...}
    schema = StructType([
        StructField("type", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField("query", StringType(), True),
        StructField("ts", StringType(), True),
    ])

    print(f"Reading from Kafka: {kafka_config['kafka.bootstrap.servers']}, topic=primary.client_requests")

    read_options = dict(kafka_config)
    read_options["startingOffsets"] = "earliest"

    stream_df = (spark.readStream
                 .format("kafka")
                 .options(**read_options)
                 .option("subscribe", "primary.client_requests")
                 .load()
                 .selectExpr("CAST(value AS STRING) as json")
                 .select(from_json(col("json"), schema).alias("data"))
                 .select("data.*")
                 .filter(col("type").isNotNull()))

    query = (stream_df.writeStream
             .foreachBatch(lambda batch_df, batch_id: write_batch(batch_df, batch_id, kafka_config))
             .outputMode("append")
             .option("checkpointLocation", "/tmp/checkpoint-spark-analytics")
             .start())

    print("Analytics system started. Waiting for data...")
    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        print("Stopping analytics...")
        query.stop()
        spark.stop()


if __name__ == "__main__":
    main()
