from abc import ABC, abstractmethod
from typing import Dict

from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.datastream import DataStream, StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import (
    KafkaOffsetsInitializer,
    KafkaSource,
    KafkaTopicPartition,
)
from pyflink.java_gateway import get_gateway

from process_config import ProcessServiceConfig


def parse_offsets(topic: str, raw_offsets: str) -> Dict[KafkaTopicPartition, int]:
    # format: "0:123,1:456"
    result: Dict[KafkaTopicPartition, int] = {}
    for item in raw_offsets.split(","):
        cleaned = item.strip()
        if not cleaned:
            continue
        partition_text, offset_text = cleaned.split(":")
        result[KafkaTopicPartition(topic, int(partition_text))] = int(offset_text)
    return result


def build_offsets_initializer(
    topic: str,
    start_mode: str,
    raw_offsets: str,
    committed_offset_reset: str,
) -> KafkaOffsetsInitializer:
    def committed_offsets_with_reset(reset_mode: str) -> KafkaOffsetsInitializer:
        gateway = get_gateway()
        j_offsets_initializer = (
            gateway.jvm.org.apache.flink.connector.kafka.source.enumerator.initializer
            .OffsetsInitializer
        )
        reset_value = reset_mode.strip().upper()
        j_reset_strategy = gateway.jvm.org.apache.kafka.clients.consumer.OffsetResetStrategy.valueOf(
            reset_value
        )
        return KafkaOffsetsInitializer(j_offsets_initializer.committedOffsets(j_reset_strategy))

    mode = start_mode.strip().lower()
    if mode == "earliest":
        return KafkaOffsetsInitializer.earliest()
    if mode == "latest":
        return KafkaOffsetsInitializer.latest()
    if mode == "committed":
        return committed_offsets_with_reset(committed_offset_reset)
    if mode == "specific":
        if not raw_offsets.strip():
            raise ValueError("KAFKA_START_OFFSETS is required when KAFKA_START_MODE=specific")
        return KafkaOffsetsInitializer.offsets(parse_offsets(topic, raw_offsets))

    raise ValueError(
        "Unsupported KAFKA_START_MODE. Use one of: earliest, latest, committed, specific"
    )


class SourceBackend(ABC):
    @abstractmethod
    def build_stream(self, env: StreamExecutionEnvironment, cfg: ProcessServiceConfig) -> DataStream:
        pass


class KafkaSourceBackend(SourceBackend):
    def build_stream(self, env: StreamExecutionEnvironment, cfg: ProcessServiceConfig) -> DataStream:
        env.add_jars(
            "file:///workspace/flink-jars/flink-connector-kafka-3.0.2-1.18.jar",
            "file:///workspace/flink-jars/kafka-clients-3.4.0.jar",
        )
        offsets_initializer = build_offsets_initializer(
            cfg.stream_name,
            cfg.start_mode,
            cfg.start_offsets,
            cfg.committed_offset_reset,
        )
        source_builder = (
            KafkaSource.builder()
            .set_bootstrap_servers(cfg.bootstrap_servers)
            .set_topics(cfg.stream_name)
            .set_group_id(cfg.group_id)
            .set_starting_offsets(offsets_initializer)
            .set_value_only_deserializer(SimpleStringSchema())
        )

        source = source_builder.build()
        return env.from_source(source, WatermarkStrategy.no_watermarks(), "Kafka Source")


class KinesisSourceBackend(SourceBackend):
    def build_stream(self, env: StreamExecutionEnvironment, cfg: ProcessServiceConfig) -> DataStream:
        raise NotImplementedError(
            "Kinesis source backend is not implemented yet. "
            "Set SOURCE_BACKEND=kafka for now."
        )


def create_source_backend(backend_name: str) -> SourceBackend:
    if backend_name == "kafka":
        return KafkaSourceBackend()
    if backend_name == "kinesis":
        return KinesisSourceBackend()
    raise ValueError(f"Unsupported SOURCE_BACKEND: {backend_name}")
