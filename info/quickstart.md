# Quickstart

## Goal

Run the process service end-to-end:

`Producer -> Kafka -> Flink Process Job -> Processed Storage / DLQ Storage`

## Steps

1. Start infra:
```bash
cd /workspace/docker
docker compose up -d --build
```

2. Enter JobManager:
```bash
docker exec -it flink-jobmanager bash
```

3. Run Flink job (checkpoint-aware):
```bash
/workspace/flink-jobs/run_process_job.sh
```

4. Run producer from another terminal:
```bash
python /workspace/producer/producer.py
```

5. Observe outputs:
```bash
docker logs -f flink-taskmanager
ls -la /workspace/storage/processed
ls -la /workspace/storage/dlq
```

## Fresh Start Mode

Use this when you do not want restore-from-checkpoint:

```bash
FORCE_FRESH_START=true /workspace/flink-jobs/run_process_job.sh
```
