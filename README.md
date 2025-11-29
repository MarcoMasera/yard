# Real-Time Streaming ETL – Take Home Assignment

This repo contains a small end-to-end streaming ETL pipeline:

- JSON events produced to **Kafka**
- Real-time processing with **Spark Structured Streaming**
- Storage in **Parquet** on an **S3-compatible** object store (**MinIO**)
- Batch analytics job computing a **95th percentile** over event durations

The implementation is focused on clarity, reproducibility and ease of local execution.

---

## 1. High-Level Architecture

**Components**

- **Ingestion layer**
  - Apache Kafka (Docker) as message broker  
  - Python producer (`confluent-kafka`) emitting JSON events

- **Streaming processing layer**
  - Spark Structured Streaming (PySpark 3.5.1)
  - JSON parsing, validation and basic cleaning
  - Two sinks:
    - **Raw events** (clean, non-aggregated)
    - **Windowed aggregations** (per device type, 1-minute window)

- **Storage layer**
  - MinIO running in Docker, used as an **S3-compatible** object store
  - Data written as **Parquet** files under S3A-style paths

- **Batch analytics**
  - Spark batch jobs:
    - `validate_minio_output.py` – schema/preview of the stream output
    - `percentile_query.py` – required 95th percentile query over event_duration

Monitoring (Prometheus / Grafana) is left as a possible next step.

---

## 2. Event Schema

All events published to Kafka follow this JSON schema:

```json
{
  "event_id": "uuid string",
  "device_id": "string",
  "device_type": "string",
  "event_timestamp": "ISO-8601 string, e.g. 2025-11-29T10:55:12Z",
  "event_duration": 123.45,
  "value": 10.5
}
```

Fields:

- `event_id` – unique identifier for the event (UUID)
- `device_id` – identifier of the emitting device
- `device_type` – device category (e.g. `sensor_temp`, `sensor_humidity`, `sensor_motion`)
- `event_timestamp` – event time in ISO-8601 format (UTC)
- `event_duration` – numeric duration (arbitrary units, e.g. ms)
- `value` – numeric measurement associated with the event

This schema is used consistently by the producer and by the Spark jobs.

---

## 3. Project Structure

```text
.
├── ingestion
│   ├── docker-compose.yml        # Kafka + Zookeeper + Kafka UI
│   └── producer
│       ├── producer.py           # JSON event producer (confluent-kafka)
│       └── requirements.txt
│
├── storage
│   └── docker-compose.yml        # MinIO (S3-compatible storage)
│
├── streaming
│   └── streaming_job.py          # Spark Structured Streaming job
│
├── spark_batch
│   ├── validate_minio_output.py  # Read/inspect Parquet from MinIO
│   └── percentile_query.py       # 95th percentile query (batch)
│
├── infra
│   └── create_s3_bucket.sh       # (Optional) helper for real AWS S3
│
├── run_commands.txt              # Handy list of commands / runbook
└── README.md
```

---

## 4. Prerequisites

- **Python** 3.10+ (tested with 3.12)
- **Java** JDK 8+ (required by Spark)
- **Docker** and **Docker Compose**
- `pip` for Python package management

For local development a Python virtual environment is recommended:

```bash
python -m venv .venv
# Windows
.\.venv\Scripts\activate
# Linux / macOS
source .venv/bin/activate
```

---

## 5. Installation

From the project root:

```bash
# Activate venv
.\.venv\Scripts\activate   # or `source .venv/bin/activate` on Unix-like systems

# Install producer dependencies
pip install -r ingestion/producer/requirements.txt

# Install PySpark
pip install pyspark==3.5.1
```

Pyspark pulls the core Spark and Hadoop jars; additional connectors (Kafka, S3A) are added via `spark.jars.packages` in the Spark jobs.

---

## 6. Running the Pipeline

### 6.1 Start MinIO (S3-compatible storage)

```bash
cd storage
docker compose up -d
docker compose ps
```

MinIO console (to create and inspect buckets):

- URL: `http://localhost:9001`
- Default credentials:
  - **user**: `admin`
  - **password**: `admin12345`

Create a bucket named:

```text
streaming-output
```

This bucket is used for all S3A paths in the pipeline.

---

### 6.2 Start Kafka

```bash
cd ingestion
docker compose up -d
docker compose ps
```

Kafka UI is available at:

- `http://localhost:8080`  
  (cluster `local`, topic `iot-events` is created by the producer)

---

### 6.3 Start the Kafka Producer

```bash
cd ingestion/producer
.\..\..\ .venv\Scripts\activate   # adjust path if needed, or activate venv before
python producer.py
```

What it does:

- connects to Kafka at `localhost:29092`
- continuously generates random IoT-like events matching the JSON schema
- sends them to the topic **`iot-events`** with **at-least-once** semantics:
  - `enable.idempotence=true`
  - `acks=all`
  - `retries=5`

Stop with `Ctrl + C`.

---

### 6.4 Start the Streaming Job (Spark Structured Streaming)

```bash
cd streaming
.\..\ .venv\Scripts\activate   # activate venv if not already
python streaming_job.py
```

The job:

- consumes the `iot-events` Kafka topic using **Spark Structured Streaming**
- parses the JSON payload into typed columns
- cleans invalid records:
  - casts `event_timestamp` -> `timestamp` (`event_time`)
  - drops nulls
  - filters non-positive durations
- applies a watermark on `event_time` (2 minutes) for late data handling
- writes **two sinks** to MinIO, both in **Parquet**:

1. **Raw events**

   - Path: `s3a://streaming-output/raw_events`
   - Checkpoints: `s3a://streaming-output/checkpoints/raw_events`
   - Mode: `append`
   - Contains one row per clean event, with `event_time` and the original fields

2. **Aggregated metrics**

   - Path: `s3a://streaming-output/stream_output`
   - Checkpoints: `s3a://streaming-output/checkpoints/stream_output`
   - Mode: `append`
   - Aggregation:
     - 1-minute tumbling window on `event_time`
     - grouped by `device_type`
     - computes `avg(value)` as `avg_value`

Spark is configured to talk to MinIO through the S3A connector:

- `org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1`
- `org.apache.hadoop:hadoop-aws:3.3.4`
- S3A settings in `spark._jsc.hadoopConfiguration()`:
  - endpoint `http://localhost:9000`
  - access key `admin`
  - secret key `admin12345`
  - `fs.s3a.path.style.access=true`

Stop the job with `Ctrl + C`.

---

## 7. Storage Layout on MinIO

After a few minutes with producer + streaming job running, the bucket `streaming-output` will contain:

- `raw_events/` – raw cleaned events (Parquet)
- `stream_output/` – windowed aggregations (Parquet)
- `checkpoints/...` – Structured Streaming checkpoints
- `analytics/p95_event_duration/` – CSV outputs from the batch percentile query (see below)

You can inspect all of this via the MinIO web console.

---

## 8. Batch Validation – Reading Parquet from MinIO

Script: `spark_batch/validate_minio_output.py`

Run:

```bash
cd spark_batch
.\..\ .venv\Scripts\activate
python validate_minio_output.py
```

What it does:

- Configures Spark with `hadoop-aws` and MinIO S3A settings
- Reads Parquet files from:

  ```text
  s3a://streaming-output/stream_output
  ```

- Prints:
  - the schema of the aggregated dataset
  - a sample of rows

This validates that the streaming job is correctly writing Parquet files in a Spark-readable format.

---

## 9. Spark Query – 95th Percentile of `event_duration`

This section implements the query requested in the assignment:

> “Compute the 95th percentile of `event_duration` per device type per day, excluding outliers beyond 3 standard deviations from the daily mean. Include only device types that had at least 500 distinct events per day.”

The implementation lives in `spark_batch/percentile_query.py`.

### 9.1 Input Data

The query uses the **raw events** written by the streaming job:

- Input path: `s3a://streaming-output/raw_events`

Each row contains at least:

- `event_id` (unique per event)
- `device_type`
- `event_time` (Spark timestamp)
- `event_duration` (double)

The script derives a date column:

```python
event_date = to_date(event_time)
```

### 9.2 Outlier Removal

For each `(device_type, event_date)` group:

1. Compute:
   - `mean_duration = avg(event_duration)`
   - `std_duration = stddev_samp(event_duration)`
   - `events_per_day = count_distinct(event_id)`

2. Join these statistics back to the raw events.

3. Filter outliers:

```sql
ABS(event_duration - mean_duration) <= 3 * std_duration
```

Rows with `std_duration` null are discarded.

### 9.3 95th Percentile Computation

On the filtered dataset, group again by `(device_type, event_date)` and compute:

- `p95_event_duration = percentile_approx(event_duration, 0.95)`
- `events_after_filter = count_distinct(event_id)`

`percentile_approx` is used to keep the computation efficient and compatible with large datasets.

### 9.4 Device Type Filter (≥ 500 Events / Day)

The final result keeps only those `(device_type, event_date)` pairs where:

```sql
events_per_day >= 500
```

Note: the threshold is applied on the **total** number of events per day (before outlier removal), as required.

### 9.5 Output Format

The script writes the result as CSV to:

```text
s3a://streaming-output/analytics/p95_event_duration
```

Columns:

- `event_date`
- `device_type`
- `p95_event_duration`
- `events_per_day`
- `events_after_filter`

The data is coalesced to a single partition to produce a single CSV file (plus `_SUCCESS`), which serves as the “validation file” required by the assignment.

To run the query:

```bash
cd spark_batch
.\..\ .venv\Scripts\activate
python percentile_query.py
```

---

## 10. Adapting from MinIO to Real AWS S3

The pipeline uses MinIO to emulate S3 locally. To run on **real AWS S3** instead:

1. Replace the MinIO `docker-compose.yml` with AWS S3 (no Docker needed).
2. Remove the MinIO-specific S3A configuration:

   - drop `fs.s3a.endpoint`
   - drop `SimpleAWSCredentialsProvider`

3. Use the default AWS provider chain:

   ```python
   hconf.set("fs.s3a.aws.credentials.provider",
             "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
   ```

4. Configure credentials and region via `aws configure` or environment variables.
5. Replace MinIO bucket names with your S3 bucket names in all `s3a://...` paths.

Core logic, schemas and formats remain unchanged.

---

## 11. Notes and Possible Extensions

- **Monitoring & observability**  
  The streaming job currently relies on Spark’s standard logs and UI.  
  A natural extension would be:
  - Prometheus + JMX exporter for Kafka and Spark
  - Grafana dashboards for ingestion rate, consumer lag, processing latency and error counts.

- **Testing & CI**  
  For brevity, automated tests and CI workflows are not included, but the code is structured so that:
  - producer logic (`build_event`) can be unit-tested
  - batch queries can be validated against small local Parquet samples

- **Exactly-once semantics**  
  The producer uses idempotence and retries to achieve strong at-least-once guarantees.  
  Full end-to-end exactly-once would require transactional writes or idempotent sink logic, which is beyond the scope of this exercise but compatible with the chosen stack.

---

## 12. How to Run – Quick Recap

Typical order to run the whole pipeline:

1. **Start MinIO**

   ```bash
   cd storage
   docker compose up -d
   ```

2. **Start Kafka**

   ```bash
   cd ingestion
   docker compose up -d
   ```

3. **Start producer**

   ```bash
   cd ingestion/producer
   python producer.py
   ```

4. **Start streaming job**

   ```bash
   cd streaming
   python streaming_job.py
   ```

5. (Optional) Validate aggregated Parquet:

   ```bash
   cd spark_batch
   python validate_minio_output.py
   ```

6. **Run 95th percentile query**:

   ```bash
   cd spark_batch
   python percentile_query.py
   ```

7. Stop producer and streaming job with `Ctrl + C`, then stop Docker services with `docker compose down`.

This completes the requested streaming ETL pipeline, S3-compatible storage integration, and analytic Spark query.


## 13. Monitoring (Optional)

For this take-home I focused mainly on the data pipeline, but I also added a minimal
monitoring stack based on **Prometheus** and **Grafana**, located under `monitoring/`.

### 13.1 Components

- `monitoring/docker-compose.yml`
  - starts Prometheus and Grafana
- `monitoring/prometheus.yml`
  - basic Prometheus configuration with two scrape jobs:
    - `spark`  – intended to scrape Spark driver metrics on `host.docker.internal:4040`
    - `kafka-ui` – simple HTTP check against Kafka UI on `host.docker.internal:8080`

In a production environment I would expose proper Prometheus metrics from Spark and Kafka
via JMX exporters or native Prometheus endpoints. For this exercise the configuration is
kept intentionally simple and mainly illustrates how the stack would be wired together.

### 13.2 How to run

From the project root:

```bash
cd monitoring
docker compose up -d
docker compose ps