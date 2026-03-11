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
    committed_offset_reset: str
    parallelism: int
    enable_checkpointing: bool
    checkpoint_interval_ms: int
    checkpoint_storage_dir: str
    checkpoint_timeout_ms: int
    min_pause_between_checkpoints_ms: int
    max_concurrent_checkpoints: int
    tolerable_checkpoint_failures: int
    externalized_checkpoint_cleanup: str
    restart_attempts: int
    restart_delay_ms: int
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
        start_mode=os.getenv("KAFKA_START_MODE", "committed"),
        start_offsets=os.getenv("KAFKA_START_OFFSETS", ""),
        committed_offset_reset=os.getenv("KAFKA_COMMITTED_OFFSET_RESET", "latest"),
        parallelism=int(os.getenv("FLINK_PARALLELISM", "1")),
        enable_checkpointing=os.getenv("ENABLE_CHECKPOINTING", "true").lower() == "true",
        checkpoint_interval_ms=int(os.getenv("CHECKPOINT_INTERVAL_MS", "10000")),
        checkpoint_storage_dir=os.getenv(
            "CHECKPOINT_STORAGE_DIR",
            "file:///workspace/flink-checkpoints",
        ),
        checkpoint_timeout_ms=int(os.getenv("CHECKPOINT_TIMEOUT_MS", "60000")),
        min_pause_between_checkpoints_ms=int(
            os.getenv("MIN_PAUSE_BETWEEN_CHECKPOINTS_MS", "3000")
        ),
        max_concurrent_checkpoints=int(os.getenv("MAX_CONCURRENT_CHECKPOINTS", "1")),
        tolerable_checkpoint_failures=int(os.getenv("TOLERABLE_CHECKPOINT_FAILURES", "3")),
        externalized_checkpoint_cleanup=os.getenv(
            "EXTERNALIZED_CHECKPOINT_CLEANUP",
            "RETAIN_ON_CANCELLATION",
        ).strip().upper(),
        restart_attempts=int(os.getenv("RESTART_ATTEMPTS", "10")),
        restart_delay_ms=int(os.getenv("RESTART_DELAY_MS", "10000")),
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
        storage_bucket_format=os.getenv("STORAGE_BUCKET_FORMAT", "yyyyMMddHHmmss"),
    )
