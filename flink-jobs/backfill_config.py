import os
from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True)
class BackfillConfig:
    """Configuration for backfill batch processing job."""
    
    # Input DLQ locations
    producer_dlq_path: str
    process_dlq_path: str
    
    # Date range filtering
    backfill_from: str  # Format: YYYY-MM-DD
    backfill_to: str    # Format: YYYY-MM-DD
    
    # Recovery strategy
    strategy: str  # "all" | "smart_filtered"
    
    # Schema registry
    schema_registry_dir: str
    schema_dataset: str
    schema_bootstrap_file: str
    schema_bootstrap_name: str
    
    # Output paths
    processed_output_path: str
    backfill_sink_path: str
    dead_sink_path: str
    metrics_output_path: str
    
    # Kafka for producer retry
    kafka_bootstrap_servers: str
    kafka_topic: str
    
    # Processing behavior
    max_retries: int
    enable_schema_healing: bool
    enable_data_coercion: bool


def load_backfill_config() -> BackfillConfig:
    """Load backfill configuration from environment variables."""
    return BackfillConfig(
        producer_dlq_path=os.getenv(
            "BACKFILL_PRODUCER_DLQ_PATH",
            "/workspace/dlq/kafka_failed_events.jsonl"
        ),
        process_dlq_path=os.getenv(
            "BACKFILL_PROCESS_DLQ_PATH",
            "/workspace/storage/dlq/process_dlq_events.jsonl"
        ),
        backfill_from=os.getenv("BACKFILL_FROM", "2026-03-01"),
        backfill_to=os.getenv("BACKFILL_TO", "2026-03-14"),
        strategy=os.getenv("BACKFILL_STRATEGY", "smart_filtered"),
        schema_registry_dir=os.getenv(
            "SCHEMA_REGISTRY_DIR",
            "/workspace/flink-jobs/schema_registry"
        ),
        schema_dataset=os.getenv("SCHEMA_DATASET", "default_event"),
        schema_bootstrap_file=os.getenv(
            "SCHEMA_BOOTSTRAP_FILE",
            "/workspace/flink-jobs/schema_registry.json"
        ),
        schema_bootstrap_name=os.getenv("SCHEMA_BOOTSTRAP_NAME", "default_event_v1"),
        processed_output_path=os.getenv(
            "PROCESSED_OUTPUT_PATH",
            "/workspace/storage/processed/processed_events.jsonl"
        ),
        backfill_sink_path=os.getenv(
            "BACKFILL_SINK_PATH",
            "/workspace/storage/backfilled/backfilled_events.jsonl"
        ),
        dead_sink_path=os.getenv(
            "DEAD_SINK_PATH",
            "/workspace/storage/dead_sink/unrecoverable_events.jsonl"
        ),
        metrics_output_path=os.getenv(
            "BACKFILL_METRICS_PATH",
            "/workspace/storage/backfill_metrics/metrics.json"
        ),
        kafka_bootstrap_servers=os.getenv(
            "KAFKA_BOOTSTRAP_SERVERS",
            "host.docker.internal:9092"
        ),
        kafka_topic=os.getenv("KAFKA_TOPIC", "test-topic"),
        max_retries=int(os.getenv("BACKFILL_MAX_RETRIES", "3")),
        enable_schema_healing=os.getenv("ENABLE_SCHEMA_HEALING", "true").lower() == "true",
        enable_data_coercion=os.getenv("ENABLE_DATA_COERCION", "true").lower() == "true",
    )
