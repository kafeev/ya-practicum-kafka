import json

from kafka import KafkaProducer
from kafka.serializer import Serializer

from config import config
from logger import get_logger

log = get_logger(__name__)


class JsonSerializer(Serializer):
    def serialize(self, topic, value):
        return json.dumps(value).encode("utf-8")


class JsonProducer:
    """Продюсер JSON-сообщений.

    Kafka-продюсер инициализируется в конструкторе, вне точки запуска.
    """

    def __init__(self) -> None:
        self._producer = KafkaProducer(
            bootstrap_servers=config.kafka_brokers,
            security_protocol=config.security_protocol,
            sasl_mechanism=config.sasl_mechanism,
            sasl_plain_username=config.kafka_user,
            sasl_plain_password=config.kafka_password,
            ssl_cafile=config.ca_file,
            api_version=(2, 8, 0),
            value_serializer=JsonSerializer(),
        )

    def send(self, topic: str | None = None, key: bytes = b"key-1",
             value: dict | None = None) -> None:
        topic = topic or config.kafka_topic
        value = value or {"message": "hello from producer"}

        self._producer.send(topic, key=key, value=value)
        self._producer.flush()
        log.info("Message sent")

    def close(self) -> None:
        self._producer.close()


if __name__ == "__main__":
    producer = JsonProducer()
    try:
        producer.send()
    finally:
        producer.close()
