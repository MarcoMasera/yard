import os
from pyspark.sql import SparkSession

# MinIO / S3A configuration (same as streaming job)
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "admin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "admin12345")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "streaming-output")

# Aggregated output path written by the streaming job
OUTPUT_PATH = f"s3a://{MINIO_BUCKET}/stream_output"


def get_spark() -> SparkSession:
    """
    Create SparkSession configured to read from MinIO via S3A.
    """
    packages = "org.apache.hadoop:hadoop-aws:3.3.4"

    spark = (
        SparkSession.builder
        .appName("ValidateMinIOOutput")
        .config("spark.jars.packages", packages)
        .getOrCreate()
    )

    # S3A connector setup for MinIO
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


def main() -> None:
    """
    Read Parquet files from MinIO and print schema + sample rows.
    """
    print(f"Reading aggregated output from: {OUTPUT_PATH}")
    spark = get_spark()

    df = spark.read.parquet(OUTPUT_PATH)

    print("Schema:")
    df.printSchema()

    print("Sample rows:")
    df.show(20, truncate=False)


if __name__ == "__main__":
    main()
