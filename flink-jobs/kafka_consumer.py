from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSource
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer

env = StreamExecutionEnvironment.get_execution_environment()
env.set_parallelism(1)

def debug(x):
    print("EVENT:", x)
    return x

env.add_jars(
    "file:///workspace/flink-jars/flink-connector-kafka-3.0.2-1.18.jar",
    "file:///workspace/flink-jars/kafka-clients-3.4.0.jar"
)

source = KafkaSource.builder() \
    .set_bootstrap_servers("host.docker.internal:9092") \
    .set_topics("test-topic") \
    .set_group_id("flink-group") \
    .set_starting_offsets(KafkaOffsetsInitializer.earliest()) \
    .set_value_only_deserializer(SimpleStringSchema()) \
    .build()

ds = env.from_source(source, WatermarkStrategy.no_watermarks(), "Kafka Source")

# ds.print()
ds.map(debug).print()

env.execute("Kafka Flink Job")