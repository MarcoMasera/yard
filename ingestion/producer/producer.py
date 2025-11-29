import json
import random
import time
import uuid
from datetime import datetime, timezone

from confluent_kafka import Producer

# Kafka topic and broker (exposed by docker-compose)
TOPIC_NAME = "iot-events"
BOOTSTRAP_SERVERS = "localhost:29092"

# Simple catalog of devices / types used to generate sample data
DEVICE_TYPES = ["sensor_temp", "sensor_humidity", "sensor_motion"]
DEVICE_IDS = [f"dev-{i:03d}" for i in range(1, 51)]  # 50 devices


def build_event() -> dict:
    """
    Build a single IoT-like event, matching the schema expected by Spark.
    """
    now = datetime.now(timezone.utc)

    device_id = random.choice(DEVICE_IDS)
    device_type = random.choice(DEVICE_TYPES)

    event_duration = round(random.uniform(10.0, 500.0), 3)
    value = round(random.uniform(0.0, 100.0), 2)

    return {
        "event_id": str(uuid.uuid4()),
        "device_id": device_id,
        "device_type": device_type,
        "event_timestamp": now.isoformat(),
        "event_duration": event_duration,
        "value": value,
    }


def delivery_report(err, msg) -> None:
    """
    Called for each message to report delivery result.
    Only errors are logged.
    """
    if err is not None:
        print(f"Delivery failed for record {msg.key()}: {err}")


def main() -> None:
    """
    Create an idempotent Kafka producer and send events in a loop.
    """
    conf = {
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        # Safer delivery guarantees (at-least-once / near exactly-once)
        "enable.idempotence": True,
        "acks": "all",
        "retries": 5,
        "linger.ms": 10,
    }

    producer = Producer(conf)

    print(f"Starting producer on topic '{TOPIC_NAME}'...")
    try:
        while True:
            event = build_event()

            producer.produce(
                TOPIC_NAME,
                value=json.dumps(event).encode("utf-8"),
                on_delivery=delivery_report,
            )

            # Serve delivery callbacks and flush internal buffer gradually
            producer.poll(0)

            print(f"Sent: {event}")
            time.sleep(0.2)
    except KeyboardInterrupt:
        print("Stopping producer...")
    finally:
        print("Flushing pending messages...")
        producer.flush()


if __name__ == "__main__":
    main()
