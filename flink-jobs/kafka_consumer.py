from functools import partial

from pyflink.common import RestartStrategies, Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.checkpoint_config import ExternalizedCheckpointCleanup

from process_config import load_config
from process_sinks import create_jsonl_file_sink
from process_sources import create_source_backend
from process_transform import (
    is_invalid_envelope,
    is_valid_envelope,
    process_raw_event,
    to_dlq_json,
    to_processed_json,
)


def main() -> None:
    cfg = load_config()

    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(cfg.parallelism)
    env.set_restart_strategy(
        RestartStrategies.fixed_delay_restart(
            cfg.restart_attempts,
            cfg.restart_delay_ms,
        )
    )

    if cfg.enable_checkpointing:
        env.enable_checkpointing(cfg.checkpoint_interval_ms)
        checkpoint_config = env.get_checkpoint_config()
        checkpoint_config.set_checkpoint_storage_dir(cfg.checkpoint_storage_dir)
        checkpoint_config.set_checkpoint_timeout(cfg.checkpoint_timeout_ms)
        checkpoint_config.set_min_pause_between_checkpoints(
            cfg.min_pause_between_checkpoints_ms
        )
        checkpoint_config.set_max_concurrent_checkpoints(cfg.max_concurrent_checkpoints)
        checkpoint_config.set_tolerable_checkpoint_failure_number(
            cfg.tolerable_checkpoint_failures
        )

        cleanup_mode = (
            ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION
            if cfg.externalized_checkpoint_cleanup == "RETAIN_ON_CANCELLATION"
            else ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION
        )
        checkpoint_config.enable_externalized_checkpoints(cleanup_mode)

    source_backend = create_source_backend(cfg.source_backend)
    raw_stream = source_backend.build_stream(env, cfg)

    transform_fn = partial(
        process_raw_event,
        registry_dir=cfg.schema_registry_dir,
        dataset=cfg.schema_dataset,
        stream_name=cfg.stream_name,
        bootstrap_file=cfg.schema_bootstrap_file,
        bootstrap_name=cfg.schema_bootstrap_name,
    )
    enveloped = raw_stream.map(transform_fn, output_type=Types.STRING())

    valid_stream = (
        enveloped
        .filter(is_valid_envelope)
        .map(to_processed_json, output_type=Types.STRING())
    )
    dlq_stream = (
        enveloped
        .filter(is_invalid_envelope)
        .map(to_dlq_json, output_type=Types.STRING())
    )

    valid_stream.sink_to(
        create_jsonl_file_sink(cfg.processed_output_path, cfg.storage_bucket_format)
    ).name(
        "Processed Storage Sink"
    )
    dlq_stream.sink_to(
        create_jsonl_file_sink(cfg.dlq_output_path, cfg.storage_bucket_format)
    ).name("DLQ Storage Sink")

    valid_stream.print()
    dlq_stream.print()

    env.execute("Process Service Job")


if __name__ == "__main__":
    main()
