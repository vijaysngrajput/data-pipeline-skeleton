import json
import os
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Dict, Optional

from kafka import KafkaProducer


@dataclass
class PublishResult:
    success: bool
    error: Optional[str] = None
    topic: Optional[str] = None
    partition: Optional[int] = None
    offset: Optional[int] = None


class StreamingBackend(ABC):
    @abstractmethod
    def publish(self, event: Dict[str, Any]) -> PublishResult:
        pass

    @abstractmethod
    def close(self) -> None:
        pass


class KafkaStreamingBackend(StreamingBackend):
    def __init__(self) -> None:
        self.topic = os.getenv("KAFKA_TOPIC", "test-topic")
        bootstrap_servers = os.getenv(
            "KAFKA_BOOTSTRAP_SERVERS",
            "host.docker.internal:9092",
        )
        ack_timeout_sec = int(os.getenv("KAFKA_ACK_TIMEOUT_SEC", "5"))

        self.ack_timeout_sec = ack_timeout_sec
        self._producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            request_timeout_ms=3000,
            max_block_ms=5000,
            api_version_auto_timeout_ms=3000,
            retries=0,
            api_version=(0, 10),
        )

    def publish(self, event: Dict[str, Any]) -> PublishResult:
        try:
            future = self._producer.send(self.topic, event)
            metadata = future.get(timeout=self.ack_timeout_sec)
            return PublishResult(
                success=True,
                topic=metadata.topic,
                partition=metadata.partition,
                offset=metadata.offset,
            )
        except Exception as err:
            return PublishResult(success=False, error=str(err), topic=self.topic)

    def close(self) -> None:
        self._producer.flush()
        self._producer.close()


class KinesisStreamingBackend(StreamingBackend):
    def __init__(self) -> None:
        raise NotImplementedError(
            "Kinesis backend is not implemented yet. "
            "Set STREAMING_BACKEND=kafka for now."
        )

    def publish(self, event: Dict[str, Any]) -> PublishResult:
        return PublishResult(success=False, error="kinesis_not_implemented")

    def close(self) -> None:
        return None


def create_streaming_backend(backend_name: str) -> StreamingBackend:
    if backend_name == "kafka":
        return KafkaStreamingBackend()
    if backend_name == "kinesis":
        return KinesisStreamingBackend()
    raise ValueError(f"Unsupported STREAMING_BACKEND: {backend_name}")
