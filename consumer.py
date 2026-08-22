import json

from kafka import KafkaConsumer, TopicPartition
from kafka.serializer import Deserializer

from config import config
from logger import get_logger

log = get_logger(__name__)


class JsonDeserializer(Deserializer):
    def deserialize(self, topic, value):
        try:
            return json.loads(value.decode("utf-8"))
        except (ValueError, AttributeError):
            # пропускаем сообщения, не являющиеся JSON
            # (например, Avro-сообщения из avro_producer.py)
            return None


class JsonConsumer:
    """Консьюмер JSON-сообщений.

    Kafka-консьюмер инициализируется в конструкторе, вне точки запуска.
    """

    def __init__(self) -> None:
        self._consumer = KafkaConsumer(
            bootstrap_servers=config.kafka_brokers,
            security_protocol=config.security_protocol,
            sasl_mechanism=config.sasl_mechanism,
            sasl_plain_username=config.kafka_user,
            sasl_plain_password=config.kafka_password,
            ssl_cafile=config.ca_file,
            api_version=(2, 8, 0),
            value_deserializer=JsonDeserializer(),
            auto_offset_reset="earliest",
        )

    def run(self) -> None:
        partitions = [
            TopicPartition(config.kafka_topic, p)
            for p in self._consumer.partitions_for_topic(config.kafka_topic)
        ]
        self._consumer.assign(partitions)
        self._consumer.seek_to_beginning(*partitions)

        log.info("Consumer started, waiting for messages...")
        while True:
            records = self._consumer.poll(timeout_ms=1000)
            for tp, messages in records.items():
                for message in messages:
                    if message.value is None:
                        continue
                    log.info("offset=%s value=%s", message.offset, message.value)

    def close(self) -> None:
        self._consumer.close()


if __name__ == "__main__":
    consumer = JsonConsumer()
    try:
        consumer.run()
    except KeyboardInterrupt:
        log.info("Interrupted by user")
    finally:
        consumer.close()
