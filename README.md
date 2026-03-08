# Data Pipeline Skeleton

A minimal **real-time data pipeline** built using:

-   Apache Kafka
-   Apache Flink (PyFlink)
-   Python Producer
-   Docker
-   VS Code Dev Containers

Architecture:

Producer → Kafka Topic → Flink Streaming Job → Console Output

------------------------------------------------------------------------

## Project Structure

    data-pipeline-skeleton
    │
    ├── producer/
    │   └── producer.py
    │
    ├── flink-jobs/
    │   └── kafka_consumer.py
    │
    ├── docker-compose.yml
    ├── .devcontainer/
    └── README.md

------------------------------------------------------------------------

## Prerequisites

Install:

-   Docker Desktop
-   VS Code
-   Dev Containers extension
-   Git

Verify installation:

``` bash
docker --version
git --version
```

------------------------------------------------------------------------

## Clone Repository

``` bash
git clone https://github.com/<your-username>/data-pipeline-skeleton.git
cd data-pipeline-skeleton
```

------------------------------------------------------------------------

## Start Kafka Infrastructure

``` bash
cd docker
docker compose up -d --build
```

Verify containers:

``` bash
docker ps
```

Access Flink UI:
```http://localhost:8081```
------------------------------------------------------------------------

## Kafka Commands

Enter Kafka container:

``` bash
docker exec -it kafka bash
```

List topics:

``` bash
kafka-topics --list --bootstrap-server localhost:9092
```

Create topic (if needed):

``` bash
kafka-topics --create --topic test-topic --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```

Consume messages:

``` bash
kafka-console-consumer --topic test-topic --bootstrap-server localhost:9092 --from-beginning
```

------------------------------------------------------------------------

## Open Dev Container

In VS Code:

    Dev Containers: Reopen in Container

Workspace path inside container:

    /workspace

------------------------------------------------------------------------

## Networking Kafka from Dev Container

Test connectivity:

``` bash
nc -zv host.docker.internal 9092
```

Expected:

    Connection to host.docker.internal 9092 succeeded

------------------------------------------------------------------------



## Run Flink Job

docker exec -it flink-jobmanager bash

Run job:

flink run -py /workspace/flink-jobs/kafka_consumer.py

## Test Run Flink Streaming Job

Inside dev container:

``` bash
python flink-jobs/kafka_consumer.py
```
------------------------------------------------------------------------

## Run Producer

Open another terminal:

``` bash
python producer/producer.py
```

------------------------------------------------------------------------

## Expected Output

Producer:

    sent: {'event_id': 0, 'event_type': 'test', 'value': 0}
    sent: {'event_id': 1, 'event_type': 'test', 'value': 100}

Flink:

    {"event_id": 0, "event_type": "test", "value": 0}
    {"event_id": 1, "event_type": "test", "value": 100}

------------------------------------------------------------------------


## View Output

docker logs -f flink-taskmanager

or

Flink UI → TaskManagers → Logs

Example:

EVENT: hello
1> hello

EVENT: order_123
1> order_123


## Debugging

Check Kafka logs:

``` bash
docker logs kafka
```

Restart infrastructure:

``` bash
docker compose down
docker compose up -d
```

------------------------------------------------------------------------

## Future Improvements

-   Schema validation
-   Dead Letter Queue (DLQ)
-   Flink checkpointing
-   Window aggregations
-   Storage sinks (S3 / Iceberg / Database)

------------------------------------------------------------------------

## License

MIT
