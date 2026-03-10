import os
from dataclasses import dataclass


@dataclass(frozen=True)
class ProcessServiceConfig:
    source_backend: str
    stream_name: str
    bootstrap_servers: str
    group_id: str
    start_mode: str
    start_offsets: str
    parallelism: int
    enable_checkpointing: bool
    checkpoint_interval_ms: int
    schema_registry_dir: str
    schema_dataset: str
    schema_bootstrap_file: str
    schema_bootstrap_name: str
    processed_output_path: str
    dlq_output_path: str
    storage_bucket_format: str


def load_config() -> ProcessServiceConfig:
    return ProcessServiceConfig(
        source_backend=os.getenv("SOURCE_BACKEND", "kafka").strip().lower(),
        stream_name=os.getenv("STREAM_NAME", os.getenv("KAFKA_TOPIC", "test-topic")),
        bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "host.docker.internal:9092"),
        group_id=os.getenv("KAFKA_GROUP_ID", "flink-group"),
        start_mode=os.getenv("KAFKA_START_MODE", "earliest"),
        start_offsets=os.getenv("KAFKA_START_OFFSETS", ""),
        parallelism=int(os.getenv("FLINK_PARALLELISM", "1")),
        enable_checkpointing=os.getenv("ENABLE_CHECKPOINTING", "true").lower() == "true",
        checkpoint_interval_ms=int(os.getenv("CHECKPOINT_INTERVAL_MS", "10000")),
        schema_registry_dir=os.getenv(
            "SCHEMA_REGISTRY_DIR",
            "/workspace/flink-jobs/schema_registry",
        ),
        schema_dataset=os.getenv("SCHEMA_DATASET", "default_event"),
        schema_bootstrap_file=os.getenv(
            "SCHEMA_BOOTSTRAP_FILE",
            "/workspace/flink-jobs/schema_registry.json",
        ),
        schema_bootstrap_name=os.getenv("SCHEMA_BOOTSTRAP_NAME", "default_event_v1"),
        processed_output_path=os.getenv(
            "PROCESSED_OUTPUT_PATH",
            "/workspace/storage/processed/processed_events.jsonl",
        ),
        dlq_output_path=os.getenv(
            "PROCESS_DLQ_OUTPUT_PATH",
            "/workspace/storage/dlq/process_dlq_events.jsonl",
        ),
        storage_bucket_format=os.getenv("STORAGE_BUCKET_FORMAT", "yyyy-MM-dd--HH"),
    )
