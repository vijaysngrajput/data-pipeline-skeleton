import json
import os
import time
from kafka import KafkaProducer
from sinks import LocalJsonlSink, build_dlq_record

print("Starting producer...")

topic = os.getenv("KAFKA_TOPIC", "test-topic")
bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "host.docker.internal:9092")
event_count = int(os.getenv("EVENT_COUNT", "5"))
dlq_dir = os.getenv("DLQ_LOCAL_DIR", "/workspace/dlq")
dlq_file = os.getenv("DLQ_LOCAL_FILE", "kafka_failed_events.jsonl")
dlq_sink = LocalJsonlSink(directory=dlq_dir, filename=dlq_file)


producer = KafkaProducer(
    bootstrap_servers=bootstrap_servers,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    request_timeout_ms=3000,
    max_block_ms=5000,
    api_version_auto_timeout_ms=3000,
    retries=0,
    api_version=(0, 10)
)

ack_count = 0
nack_count = 0

for i in range(event_count):
    event = {
        "event_id": i,
        "event_type": "test",
        "value": i * 90
    }

    try:
        future = producer.send(topic, event)
        metadata = future.get(timeout=5)
        ack_count += 1
        print(
            f"ACK event_id={event['event_id']} "
            f"topic={metadata.topic} partition={metadata.partition} offset={metadata.offset}"
        )
    except Exception as err:
        nack_count += 1
        dlq_sink.write(build_dlq_record(event, str(err), topic))
        print(f"NACK event_id={event['event_id']} reason={err}")

    time.sleep(1)


producer.flush()
producer.close()

print(f"Done. acked={ack_count}, not_acked={nack_count}, dlq_file={dlq_sink.path}")
