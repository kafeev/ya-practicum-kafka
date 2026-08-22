import json
import sr_http  # noqa: F401  (monkeypatches Schema Registry client to use requests instead of httpx)

from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient, topic_subject_name_strategy
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import SerializationContext, MessageField

from config import config
from logger import get_logger

log = get_logger(__name__)

KEY_SCHEMA = {'type': 'string'}
VALUE_SCHEMA = {
    'type': 'record',
    'name': 'Message',
    'namespace': 'practicum.kafka',
    'fields': [{'name': 'message', 'type': 'string'}],
}


class AvroProducer:
    """Продюсер Avro-сообщений.

    Вся инфраструктура (Schema Registry, сериализаторы, Kafka-продюсер)
    инициализируется в конструкторе, вне точки запуска.
    """

    def __init__(self) -> None:
        self._sr = SchemaRegistryClient({'url': config.schema_registry_url})

        subject_conf = {
            'subject.name.strategy': topic_subject_name_strategy,
            'auto.register.schemas': False,
            'use.latest.version': True,
        }
        self._key_serializer = AvroSerializer(self._sr, json.dumps(KEY_SCHEMA), conf=subject_conf)
        self._value_serializer = AvroSerializer(self._sr, json.dumps(VALUE_SCHEMA), conf=subject_conf)

        self._producer = Producer({
            'bootstrap.servers': config.kafka_brokers,
            'security.protocol': config.security_protocol,
            'sasl.mechanism': config.sasl_mechanism,
            'sasl.username': config.kafka_user,
            'sasl.password': config.kafka_password,
            'ssl.ca.location': config.ca_file,
        })

    @staticmethod
    def _delivery_report(err, msg) -> None:
        if err is not None:
            log.error("Delivery failed: %s", err)
        else:
            log.info("Message delivered to %s [%s]", msg.topic(), msg.partition())

    def send(self, topic: str | None = None, key: str = "key-1",
             value: dict | None = None) -> None:
        topic = topic or config.kafka_topic
        value = value or {'message': 'hello from avro producer'}

        self._producer.produce(
            topic=topic,
            key=self._key_serializer(key, SerializationContext(topic, MessageField.KEY)),
            value=self._value_serializer(value, SerializationContext(topic, MessageField.VALUE)),
            on_delivery=self._delivery_report,
        )
        self._producer.flush()
        log.info("Avro message sent")

    def close(self) -> None:
        self._producer.flush()


if __name__ == "__main__":
    producer = AvroProducer()
    try:
        producer.send()
    finally:
        producer.close()
