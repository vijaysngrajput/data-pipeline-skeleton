# Architecture

## Logical Flow

1. `producer/producer.py` creates events and publishes to streaming backend (Kafka now).
2. Kafka stores events in topic partitions with offsets.
3. `flink-jobs/kafka_consumer.py` runs Flink process pipeline:
   - source read
   - transform + schema validation/evolution
   - split into valid and invalid
   - sink to processed and DLQ storage

## Process Service Pipeline

`Source -> Map(process_raw_event) -> Filter(valid|invalid) -> Map(output format) -> FileSink + print`

## Schema Evolution Model

- Registry directory: `flink-jobs/schema_registry/<dataset>/vN.json`
- On new fields:
  - effective schema is built
  - new version file is created if schema changed
- Output event carries:
  - `schema_dataset`
  - `schema_version`
  - full schema used for that event

## Reliability Model

- Checkpointing enabled and externalized.
- Fixed-delay restart strategy configured.
- Checkpoint-aware launcher script restores latest checkpoint when available.
