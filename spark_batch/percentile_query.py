import os
from pyspark.sql import SparkSession, functions as F


# Config MinIO (stessa usata nello streaming)
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "admin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "admin12345")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "streaming-output")

RAW_INPUT_PATH = f"s3a://{MINIO_BUCKET}/raw_events"
RESULT_OUTPUT_PATH = f"s3a://{MINIO_BUCKET}/analytics/p95_event_duration"


def get_spark():
    packages = "org.apache.hadoop:hadoop-aws:3.3.4"

    spark = (
        SparkSession.builder
        .appName("PercentileQuery")
        .config("spark.jars.packages", packages)
        .getOrCreate()
    )

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

    print(f"Reading raw events from: {RAW_INPUT_PATH}")
    events = spark.read.parquet(RAW_INPUT_PATH)

    # ci assicuriamo di avere un timestamp e una data
    if "event_time" in events.columns:
        df = events.withColumn("event_date", F.to_date("event_time"))
    else:
        # fallback se per qualche motivo avessimo solo event_timestamp string
        df = (
            events
            .withColumn("event_time", F.to_timestamp("event_timestamp"))
            .withColumn("event_date", F.to_date("event_time"))
        )

    # filtriamo eventuali null / valori strani
    df = (
        df
        .filter(F.col("event_duration").isNotNull())
        .filter(F.col("event_duration") > 0)
        .filter(F.col("event_date").isNotNull())
    )

    # 1) statistiche base per device_type, day
    stats = (
        df.groupBy("device_type", "event_date")
          .agg(
              F.avg("event_duration").alias("mean_duration"),
              F.stddev_samp("event_duration").alias("std_duration"),
              F.countDistinct("event_id").alias("events_per_day")
          )
    )

    # 2) join con gli eventi per calcolare gli outlier
    df_with_stats = df.join(
        stats,
        on=["device_type", "event_date"],
        how="inner"
    )

    # 3) rimuovi outlier oltre 3 deviazioni standard dalla media giornaliera
    filtered = df_with_stats.filter(
        (F.col("std_duration").isNotNull()) &
        (F.abs(F.col("event_duration") - F.col("mean_duration")) <= 3 * F.col("std_duration"))
    )

    # 4) calcola il 95° percentile di event_duration per device_type e giorno
    percentiles = (
        filtered.groupBy("device_type", "event_date")
                .agg(
                    F.expr("percentile_approx(event_duration, 0.95)").alias("p95_event_duration"),
                    F.countDistinct("event_id").alias("events_after_filter")
                )
    )


    # 5) tieni solo i device_type con almeno 500 eventi distinti al giorno (sul totale, non solo filtrati)
    result = (
        percentiles.join(
            stats.select("device_type", "event_date", "events_per_day"),
            on=["device_type", "event_date"],
            how="inner"
        )
        .filter(F.col("events_per_day") >= 500)
        .select(
            "event_date",
            "device_type",
            "p95_event_duration",
            "events_per_day",
            "events_after_filter"
        )
        .orderBy("event_date", "device_type")
    )

    print("Result schema:")
    result.printSchema()
    print("Sample result rows:")
    result.show(50, truncate=False)

    # 6) scrivi il risultato in CSV (file di validazione richiesto dal test)
    print(f"Writing results to: {RESULT_OUTPUT_PATH}")
    (
        result.coalesce(1)  # comodo per avere pochi file CSV
              .write
              .mode("overwrite")
              .option("header", "true")
              .csv(RESULT_OUTPUT_PATH)
    )


if __name__ == "__main__":
    main()
