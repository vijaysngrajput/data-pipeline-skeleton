from pathlib import Path

from pyflink.common.serialization import Encoder
from pyflink.datastream.connectors import FileSink, OutputFileConfig
from pyflink.datastream.connectors.file_system import BucketAssigner
from pyflink.java_gateway import get_gateway


def _create_datetime_bucket_assigner(bucket_format: str) -> BucketAssigner:
    gateway = get_gateway()
    j_assigner = gateway.jvm.org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner(
        bucket_format
    )
    return BucketAssigner(j_assigner)


def create_jsonl_file_sink(output_path: str, bucket_format: str) -> FileSink:
    path = Path(output_path)
    base_dir = str(path.parent) if path.suffix else str(path)
    part_prefix = path.stem if path.suffix else "part"
    part_suffix = path.suffix if path.suffix else ".jsonl"

    file_config = (
        OutputFileConfig.builder()
        .with_part_prefix(part_prefix)
        .with_part_suffix(part_suffix)
        .build()
    )

    return (
        FileSink.for_row_format(
            base_path=base_dir,
            encoder=Encoder.simple_string_encoder(),
        )
        .with_bucket_assigner(_create_datetime_bucket_assigner(bucket_format))
        .with_output_file_config(file_config)
        .build()
    )
