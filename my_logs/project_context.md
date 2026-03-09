# Project Context Log

Last updated: 2026-03-08 (UTC)
Maintained by: Codex

## Purpose

This repository is a minimal end-to-end real-time data pipeline skeleton for learning and validating Kafka + Flink integration quickly.

Primary goal:
- Prove the flow `Python Producer -> Kafka -> PyFlink Consumer -> Console output`.

Secondary goals:
- Validate container networking (`host.docker.internal`).
- Keep setup simple and reproducible with Docker + VS Code Dev Containers.
- Provide a base for future enhancements (schema validation, DLQ, checkpointing, windows, external sinks).

## Current Architecture

Producer (`producer/producer.py`)
-> Kafka topic `test-topic`
-> Flink job (`flink-jobs/kafka_consumer.py`)
-> TaskManager logs / Flink UI logs

Infra:
- Zookeeper + Kafka via Confluent images.
- Flink JobManager + TaskManager via custom Docker image in `docker/flink/Dockerfile`.

## Repository Map (important files)

- `/workspace/README.md`
- `/workspace/docker/docker-compose.yml`
- `/workspace/docker/flink/Dockerfile`
- `/workspace/docker/flink/requirements.txt`
- `/workspace/.devcontainer/devcontainer.json`
- `/workspace/producer/producer.py`
- `/workspace/flink-jobs/kafka_consumer.py`
- `/workspace/flink-jars/flink-connector-kafka-3.0.2-1.18.jar`
- `/workspace/flink-jars/kafka-clients-3.4.0.jar`
- `/workspace/my_logs/commands_so_far.md`

## Known Runtime Behavior

Producer behavior:
- Connects to `host.docker.internal:9092`.
- Sends 5 JSON messages to topic `test-topic`.
- Message shape:
  - `event_id`: integer
  - `event_type`: `"test"`
  - `value`: `event_id * 90`

Flink job behavior:
- Adds Kafka connector JARs from `/workspace/flink-jars`.
- Reads from `test-topic` with group `flink-group`.
- Starts from earliest offset.
- Prints each event with a debug map and `print()` sink.
- Parallelism set to 1.

## Standard Run Procedure

1. Start infra:
   - `cd /workspace/docker`
   - `docker compose up -d --build`
2. Ensure topic exists:
   - `docker exec -it kafka bash`
   - `kafka-topics --list --bootstrap-server localhost:9092`
   - create `test-topic` if missing
3. Run Flink job:
   - `docker exec -it flink-jobmanager bash`
   - `flink run -py /workspace/flink-jobs/kafka_consumer.py`
4. Run producer:
   - `python /workspace/producer/producer.py`
5. Observe output:
   - `docker logs -f flink-taskmanager`
   - or Flink UI: `http://localhost:8081`

## Deployment Model (current)

This project is locally deployed via Docker Compose, not cloud-deployed.

- Compose orchestrates Kafka/Zookeeper/Flink services.
- Flink image is built from `docker/flink/Dockerfile`.
- Repo is bind-mounted into Flink containers as `/workspace` for rapid iteration.

## Dev Environment Notes

Dev container:
- File: `/workspace/.devcontainer/devcontainer.json`
- Base image: `mcr.microsoft.com/devcontainers/python:3.10-bullseye`
- Installs `default-jdk`, `kafka-python`, and `apache-flink==1.18.0`.
- Sets `JAVA_HOME` to `/usr/lib/jvm/java-11-openjdk-arm64`.

Potential portability risk:
- ARM-specific `JAVA_HOME` path may fail on non-ARM hosts.

## Operating Rule For Future Sessions

When asked for project understanding, start with this file first:
- `/workspace/my_logs/project_context.md`

Then only re-scan source files if:
- files changed since this log was updated, or
- user asks for a fresh/full re-audit.

