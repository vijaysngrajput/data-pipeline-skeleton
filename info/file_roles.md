# File Roles

This page describes what each important file does, its role, and how it works.

## Flink Process Service

`flink-jobs/kafka_consumer.py`
- What: main Flink job entrypoint.
- Role: orchestration layer.
- How: loads config, builds source backend, applies transform, splits valid/DLQ, writes sinks, executes job.

`flink-jobs/process_config.py`
- What: runtime configuration loader.
- Role: single typed config object for env vars.
- How: reads env vars with defaults into `ProcessServiceConfig`.

`flink-jobs/process_sources.py`
- What: source-backend module.
- Role: where ingestion starts.
- How: builds Kafka source with start mode (`earliest/latest/committed/specific`) and group settings; supports backend extension point for Kinesis.

`flink-jobs/process_transform.py`
- What: per-event processing logic.
- Role: event normalization and routing decision.
- How: parse JSON, resolve schema version, evolve schema if needed, validate event, emit envelope (`valid` or `dlq`).

`flink-jobs/process_schema.py`
- What: schema registry and validation engine.
- Role: schema governance.
- How: maintains versioned schema files, validates payloads, infers new field types, persists evolved schema versions.

`flink-jobs/process_sinks.py`
- What: sink factory.
- Role: output persistence to files.
- How: configures Flink `FileSink` with timestamp bucket folders and JSONL output naming.

`flink-jobs/run_process_job.sh`
- What: Flink job launcher script.
- Role: safe restart workflow.
- How: restores from latest checkpoint if available; supports `FORCE_FRESH_START=true`.

`flink-jobs/schema_registry.json`
- What: bootstrap schema file.
- Role: initial seed schema definition.
- How: used to create initial `v1.json` in directory-based registry if missing.

`flink-jobs/schema_registry/default_event/v1.json`, `v2.json`, `v3.json`
- What: versioned schema history.
- Role: source of truth for schema evolution.
- How: newer versions are created automatically when evolved schema differs.

## Producer Side

`producer/producer.py`
- What: event producer app.
- Role: test/source workload generator.
- How: builds events, publishes via selected streaming backend, logs ACK/NACK, writes failed sends to local DLQ sink.

`producer/streaming_backends.py`
- What: producer backend abstraction.
- Role: makes producer extensible.
- How: `StreamingBackend` interface, Kafka implementation, Kinesis placeholder.

`producer/sinks.py`
- What: producer DLQ sink utility.
- Role: persistence for publish failures.
- How: writes JSONL failure records with timestamp/reason.

## Infra and Runtime

`docker/docker-compose.yml`
- What: local stack definition.
- Role: starts Zookeeper, Kafka, Flink JobManager, Flink TaskManager.
- How: Docker Compose services and networking.

`docker/flink/Dockerfile`
- What: custom Flink image.
- Role: runtime environment for Flink containers.
- How: installs Python + Java and Python dependencies.

`docker/flink/requirements.txt`
- What: Python runtime dependencies.
- Role: ensures PyFlink and Kafka Python client availability.
- How: installed during image build.

`README.md`
- What: project-level runbook.
- Role: operational guide.
- How: setup, run commands, config, troubleshooting.
