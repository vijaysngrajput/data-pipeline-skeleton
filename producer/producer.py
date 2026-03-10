import os
import time
from typing import Any, Dict

from sinks import LocalJsonlSink, build_dlq_record
from streaming_backends import create_streaming_backend

print("Starting producer...")

streaming_backend_name = os.getenv("STREAMING_BACKEND", "kafka").strip().lower()
stream_name = os.getenv("STREAM_NAME", os.getenv("KAFKA_TOPIC", "test-topic"))
event_count = int(os.getenv("EVENT_COUNT", "5"))
dlq_dir = os.getenv("DLQ_LOCAL_DIR", "/workspace/dlq")
dlq_file = os.getenv("DLQ_LOCAL_FILE", "kafka_failed_events.jsonl")
dlq_sink = LocalJsonlSink(directory=dlq_dir, filename=dlq_file)

backend = None
backend_init_error = None
try:
    backend = create_streaming_backend(streaming_backend_name)
except Exception as err:
    backend_init_error = str(err)
    print(f"Backend init failed: {backend_init_error}")

ack_count = 0
nack_count = 0

for i in range(event_count):
    event: Dict[str, Any] = {
        "event_id": i,
        "event_type": "test",
        "value": i * 90
    }

    if backend is None:
        nack_count += 1
        reason = backend_init_error or "streaming_backend_unavailable"
        dlq_sink.write(build_dlq_record(event, reason, stream_name))
        print(f"NACK event_id={event['event_id']} reason={reason}")
        time.sleep(1)
        continue

    result = backend.publish(event)
    if result.success:
        ack_count += 1
        print(
            f"ACK event_id={event['event_id']} "
            f"topic={result.topic} partition={result.partition} offset={result.offset}"
        )
    else:
        nack_count += 1
        reason = result.error or "unknown_publish_error"
        dlq_sink.write(build_dlq_record(event, reason, stream_name))
        print(f"NACK event_id={event['event_id']} reason={reason}")

    time.sleep(1)


if backend is not None:
    backend.close()

print(f"Done. acked={ack_count}, not_acked={nack_count}, dlq_file={dlq_sink.path}")
