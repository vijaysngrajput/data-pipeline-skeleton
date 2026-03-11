# Data Pipeline Skeleton

Project documentation for fresh users and file-by-file roles:
- `/workspace/info/README.md`

A minimal real-time data pipeline built with:

- Apache Kafka
- Apache Flink (PyFlink)
- Python producer
- Docker Compose
- VS Code Dev Containers

Current Architecture:

`Producer -> Kafka topic -> Flink streaming job -> Console output`

Target Architectures:
```
                    [DLQ LAYERS]    [Schema Registry / checkpoint / validation / DLQ LAYERS]       [DATA FORMATTING]
APP  >>>         STREAMING SERVICE               >>> PROCESS SERVICE             >>> STATELESS COMPUTE >>> DASHBOARD SERVICE
                                |                    |
                                |                    |
                                |                    |              [Backfill / Idempotent Writes]
                        Streaming Sink               |________  >>> DISTRIBUTE COMPUTE SERVICE >>> STORAGE SERVICE >>> DATA WAREHOUSING SERVICE >>> BI TEAM
                                |                    |
                                |                    |
                                |                    |
                                |                    |
                        Storage Service         Storage Service
```

## Project Structure

```text
data-pipeline-skeleton
├── .devcontainer/
│   └── devcontainer.json
├── docker/
│   ├── docker-compose.yml
│   └── flink/
│       ├── Dockerfile
│       └── requirements.txt
├── flink-jars/
│   ├── flink-connector-kafka-3.0.2-1.18.jar
│   └── kafka-clients-3.4.0.jar
├── flink-jobs/
│   ├── kafka_consumer.py
│   └── schema_registry.json
├── producer/
│   └── producer.py
└── README.md
```

## What This Project Does

1. Starts Kafka + Zookeeper with Docker.
2. Starts a Flink JobManager and TaskManager with a custom image.
3. Sends JSON events to Kafka topic `test-topic` from `producer/producer.py`.
4. Process service consumes events from Kafka in `flink-jobs/kafka_consumer.py`.
5. Process service validates events against a schema registry (`flink-jobs/schema_registry.json`).
6. Valid events are emitted to storage (`/workspace/storage/processed/processed_events.jsonl`).
7. Invalid events are emitted to process DLQ storage (`/workspace/storage/dlq/process_dlq_events.jsonl`).

## Prerequisites

- Docker Desktop
- VS Code
- Dev Containers extension
- Git

Verify:

```bash
docker --version
git --version
```

## Start Infrastructure

```bash
cd docker
docker compose up -d --build
docker ps
```

Flink UI:

`http://localhost:8081`

## Kafka Setup

Enter Kafka container:

```bash
docker exec -it kafka bash
```

List topics:

```bash
kafka-topics --list --bootstrap-server localhost:9092
```

Create topic (if needed):

```bash
kafka-topics --create \
  --topic test-topic \
  --bootstrap-server localhost:9092 \
  --partitions 1 \
  --replication-factor 1
```

Consume messages manually (optional):

```bash
kafka-console-consumer \
  --topic test-topic \
  --bootstrap-server localhost:9092 \
  --from-beginning
```

## Dev Container Workflow

In VS Code: `Dev Containers: Reopen in Container`

Workspace path inside container:

`/workspace`

Quick Kafka connectivity check from dev container:

```bash
nc -zv host.docker.internal 9092
```

## Run Flink Streaming Job

Recommended (inside Flink JobManager container):

```bash
docker exec -it flink-jobmanager bash
export SOURCE_BACKEND=kafka
export STREAM_NAME=test-topic
export KAFKA_BOOTSTRAP_SERVERS=host.docker.internal:9092
export KAFKA_GROUP_ID=flink-group
export KAFKA_START_MODE=committed
export ENABLE_CHECKPOINTING=true
export SCHEMA_REGISTRY_DIR=/workspace/flink-jobs/schema_registry
export SCHEMA_DATASET=default_event
/workspace/flink-jobs/run_process_job.sh
```

Note: Running `python flink-jobs/kafka_consumer.py` directly from the dev container can be useful for local checks, but cluster execution should use `flink run`.
If you want to ignore checkpoint restore and start fresh, run:

```bash
FORCE_FRESH_START=true /workspace/flink-jobs/run_process_job.sh
```

### Process Service Configuration

Set env vars before `flink run` (or pass through Compose):

```bash
export SOURCE_BACKEND=kafka
export STREAM_NAME=test-topic
export KAFKA_BOOTSTRAP_SERVERS=host.docker.internal:9092
export KAFKA_GROUP_ID=flink-group
export FLINK_PARALLELISM=1

export ENABLE_CHECKPOINTING=true
export CHECKPOINT_INTERVAL_MS=10000
export CHECKPOINT_STORAGE_DIR=file:///workspace/flink-checkpoints
export CHECKPOINT_TIMEOUT_MS=60000
export MIN_PAUSE_BETWEEN_CHECKPOINTS_MS=3000
export MAX_CONCURRENT_CHECKPOINTS=1
export TOLERABLE_CHECKPOINT_FAILURES=3
export EXTERNALIZED_CHECKPOINT_CLEANUP=RETAIN_ON_CANCELLATION
export RESTART_ATTEMPTS=10
export RESTART_DELAY_MS=10000

export SCHEMA_REGISTRY_DIR=/workspace/flink-jobs/schema_registry
export SCHEMA_DATASET=default_event
export SCHEMA_BOOTSTRAP_FILE=/workspace/flink-jobs/schema_registry.json
export SCHEMA_BOOTSTRAP_NAME=default_event_v1

# earliest | latest | committed | specific
export KAFKA_START_MODE=committed
# used only when KAFKA_START_MODE=specific (format: "0:10,1:20")
export KAFKA_START_OFFSETS=
# used when KAFKA_START_MODE=committed and no committed offsets exist: latest | earliest | none
export KAFKA_COMMITTED_OFFSET_RESET=latest

export PROCESSED_OUTPUT_PATH=/workspace/storage/processed/processed_events.jsonl
export PROCESS_DLQ_OUTPUT_PATH=/workspace/storage/dlq/process_dlq_events.jsonl
export STORAGE_BUCKET_FORMAT=yyyyMMddHHmmss
```

Offset behavior:
- `earliest`: consume from earliest available offsets
- `latest`: consume only new events after job start
- `committed`: resume from committed consumer-group offsets (fallback via `KAFKA_COMMITTED_OFFSET_RESET` when missing)
- `specific`: start from exact partition offsets via `KAFKA_START_OFFSETS`

Source backend:
- `SOURCE_BACKEND=kafka`: uses Kafka source implementation
- `SOURCE_BACKEND=kinesis`: reserved extension point (not implemented yet)

Schema registry behavior:
- registry is directory-based: `SCHEMA_REGISTRY_DIR/<dataset>/vN.json`
- process service reads latest schema version by default
- if new fields arrive and schema allows additional fields, it writes a new schema version
- emitted records include `schema_dataset` + `schema_version` + full effective schema

## Run Producer

From another terminal (host or dev container):

```bash
python producer/producer.py
```

Producer sends 5 events:

```text
{"event_id": 0, "event_type": "test", "value": 0}
{"event_id": 1, "event_type": "test", "value": 90}
...
```

## View Flink Output

```bash
docker logs -f flink-taskmanager
```

Or via Flink UI:

`Flink UI -> TaskManagers -> Logs`

Expected style of logs:

```text
{"timestamp_utc":"...","source_stream":"test-topic","schema_name":"default_event_v1","event":{"event_id":0,"event_type":"test","value":0}}
```

Processed output file:

`/workspace/storage/processed/processed_events.jsonl`

Process DLQ output file:

`/workspace/storage/dlq/process_dlq_events.jsonl`

## Deploy Model Used Here

This repository uses local Docker deployment:

1. `docker/docker-compose.yml` orchestrates Kafka, Zookeeper, and Flink services.
2. `docker/flink/Dockerfile` builds the Flink image with Python + Java dependencies.
3. Source code is bind-mounted to `/workspace` in Flink containers for fast iteration.

## Troubleshooting

Kafka logs:

```bash
docker logs kafka
```

Restart everything:

```bash
cd docker
docker compose down
docker compose up -d --build
```

## Future Improvements

- Window aggregations
- Storage sinks (S3 / Iceberg / database)

## License

MIT
