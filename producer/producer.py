from kafka import KafkaProducer
import json
import time

print("Starting producer...")

producer = KafkaProducer(
    bootstrap_servers="host.docker.internal:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    request_timeout_ms=5000,
    api_version=(0,10)
)

print("Connected to Kafka")

for i in range(5):
    event = {
        "event_id": i,
        "event_type": "test",
        "value": i * 90
    }

    future = producer.send("test-topic", event)

    print("sent:", event)

    future.get(timeout=10)

    time.sleep(1)

producer.flush()