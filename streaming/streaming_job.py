import sys
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    window,
    avg,
    to_timestamp,
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
)

# === Kafka config ===
KAFKA_BOOTSTRAP_SERVERS = "localhost:29092"
KAFKA_TOPIC = "iot-events"

# === MinIO (S3-compatible) config ===
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "admin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "admin12345")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "streaming-output")

# Paths for raw events and aggregates
RAW_OUTPUT_PATH = f"s3a://{MINIO_BUCKET}/raw_events"
RAW_CHECKPOINT_PATH = f"s3a://{MINIO_BUCKET}/checkpoints/raw_events"

AGG_OUTPUT_PATH = f"s3a://{MINIO_BUCKET}/stream_output"
AGG_CHECKPOINT_PATH = f"s3a://{MINIO_BUCKET}/checkpoints/stream_output"


def get_spark() -> SparkSession:
    """
    Create a SparkSession configured for Kafka + S3A (MinIO).
    """
    packages = ",".join(
        [
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1",
            "org.apache.hadoop:hadoop-aws:3.3.4",
        ]
    )

    spark = (
        SparkSession.builder
        .appName("StreamingJob")
        .config("spark.jars.packages", packages)
        .getOrCreate()
    )

    # Configure S3A connector to talk to MinIO
    hconf = spark._jsc.hadoopConfiguration()
    hconf.set("fs.s3a.endpoint", MINIO_ENDPOINT)
    hconf.set("fs.s3a.access.key", MINIO_ACCESS_KEY)
    hconf.set("fs.s3a.secret.key", MINIO_SECRET_KEY)
    hconf.set("fs.s3a.path.style.access", "true")
    hconf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    hconf.set(
        "fs.s3a.aws.credentials.provider",
        "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
    )

    return spark


# Schema of the JSON payload produced to Kafka
EVENT_SCHEMA = StructType(
    [
        StructField("event_id", StringType(), True),
        StructField("device_id", StringType(), True),
        StructField("device_type", StringType(), True),
        StructField("event_timestamp", StringType(), True),
        StructField("event_duration", DoubleType(), True),
        StructField("value", DoubleType(), True),
    ]
)


def main() -> None:
    spark = get_spark()

    # === 1. Read from Kafka as a streaming DataFrame ===
    raw_kafka_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .load()
    )

    # === 2. Parse JSON value and cast to typed columns ===
    json_df = raw_kafka_df.selectExpr("CAST(value AS STRING) AS json_str")

    parsed_df = json_df.select(
        from_json(col("json_str"), EVENT_SCHEMA).alias("data")
    ).select("data.*")

    # Add proper timestamp and basic data quality filters
    df = (
        parsed_df
        .withColumn("event_time", to_timestamp("event_timestamp"))
        .filter(col("event_time").isNotNull())
        .filter(col("event_duration").isNotNull() & (col("event_duration") > 0))
    )

    # === 3. Define watermark for late events ===
    df_with_watermark = df.withWatermark("event_time", "2 minutes")

    # === 4. RAW sink: write cleaned events to MinIO ===
    raw_query = (
        df.writeStream
        .format("parquet")
        .option("path", RAW_OUTPUT_PATH)
        .option("checkpointLocation", RAW_CHECKPOINT_PATH)
        .outputMode("append")
        .trigger(processingTime="20 seconds")
        .start()
    )

    # === 5. Aggregated sink: 1-minute avg(value) per device_type ===
    agg = (
        df_with_watermark
        .groupBy(
            window(col("event_time"), "1 minute"),
            col("device_type"),
        )
        .agg(
            avg("value").alias("avg_value"),
        )
    )

    agg_query = (
        agg.writeStream
        .format("parquet")
        .option("path", AGG_OUTPUT_PATH)
        .option("checkpointLocation", AGG_CHECKPOINT_PATH)
        .outputMode("append")
        .trigger(processingTime="20 seconds")
        .start()
    )

    print("Streaming job started. Press Ctrl+C to stop.", file=sys.stderr)

    # Keep both sinks alive
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
