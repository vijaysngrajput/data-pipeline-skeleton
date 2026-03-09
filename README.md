# Data Pipeline Skeleton

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
│   └── kafka_consumer.py
├── producer/
│   └── producer.py
└── README.md
```

## What This Project Does

1. Starts Kafka + Zookeeper with Docker.
2. Starts a Flink JobManager and TaskManager with a custom image.
3. Sends JSON events to Kafka topic `test-topic` from `producer/producer.py`.
4. Consumes events from Kafka in `flink-jobs/kafka_consumer.py` and prints them.

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
flink run -py /workspace/flink-jobs/kafka_consumer.py
```

Note: Running `python flink-jobs/kafka_consumer.py` directly from the dev container can be useful for local checks, but cluster execution should use `flink run`.

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
EVENT: {"event_id":0,"event_type":"test","value":0}
1> {"event_id":0,"event_type":"test","value":0}
```

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

- Schema validation
- Dead Letter Queue (DLQ)
- Flink checkpointing
- Window aggregations
- Storage sinks (S3 / Iceberg / database)

## License

MIT
