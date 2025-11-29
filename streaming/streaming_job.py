import sys
import os
...
KAFKA_BOOTSTRAP_SERVERS = "localhost:29092"
KAFKA_TOPIC = "iot-events"

# === MinIO (S3-compatible) config ===
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "admin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "admin12345")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "streaming-output")

# path per i raw events e per le aggregazioni
RAW_OUTPUT_PATH = f"s3a://{MINIO_BUCKET}/raw_events"
RAW_CHECKPOINT_PATH = f"s3a://{MINIO_BUCKET}/checkpoints/raw_events"

AGG_OUTPUT_PATH = f"s3a://{MINIO_BUCKET}/stream_output"
AGG_CHECKPOINT_PATH = f"s3a://{MINIO_BUCKET}/checkpoints/stream_output"


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


def get_spark(app_name: str = "StreamingETL"):
    packages = ",".join([
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1",
        "org.apache.hadoop:hadoop-aws:3.3.4",
    ])

    spark = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.jars.packages", packages)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # Configurazione S3A per MinIO
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


def main():
    spark = get_spark()

    # 1. leggo il flusso raw da Kafka
    raw_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "earliest")  # per i test va bene
        .load()
    )

    # value è binario -> string
    json_stream = raw_stream.selectExpr("CAST(value AS STRING) AS json_str")

    # 2. definisco lo schema del JSON
    event_schema = StructType([
        StructField("event_id", StringType(), False),
        StructField("device_id", StringType(), False),
        StructField("device_type", StringType(), False),
        StructField("event_timestamp", StringType(), False),
        StructField("event_duration", DoubleType(), False),
        StructField("value", DoubleType(), False),
    ])

    parsed = json_stream.select(
        from_json(col("json_str"), event_schema).alias("event")
    ).select("event.*")

    # 3. casting e colonne derivate
    df = (
        parsed
        .withColumn("event_time", to_timestamp("event_timestamp"))
        .dropna(subset=["event_time", "event_duration", "value"])
        .filter(col("event_duration") > 0)
    )

    # 4. aggiungo watermark per gestire latenze
    df_with_watermark = df.withWatermark("event_time", "2 minutes")

    # 5. aggregazione windowed: media value per device_type ogni minuto
    # ================= RAW SINK =================
    raw_query = (
        df.writeStream
          .format("parquet")
          .option("path", RAW_OUTPUT_PATH)
          .option("checkpointLocation", RAW_CHECKPOINT_PATH)
          .outputMode("append")
          .trigger(processingTime="20 seconds")
          .start()
    )

    # ================= AGGREGATED SINK =================
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

    # mantieni vivi entrambi i sink
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
