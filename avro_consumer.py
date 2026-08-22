import json
import sr_http  # noqa: F401  (monkeypatches Schema Registry client to use requests instead of httpx)

from confluent_kafka import Consumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.serialization import SerializationError

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


class AvroConsumer:
    """Консьюмер Avro-сообщений.

    Вся инфраструктура (Schema Registry, десериализаторы, Kafka-консьюмер)
    инициализируется в конструкторе, вне точки запуска.
    """

    def __init__(self) -> None:
        self._sr = SchemaRegistryClient({'url': config.schema_registry_url})
        self._key_deserializer = AvroDeserializer(self._sr, json.dumps(KEY_SCHEMA))
        self._value_deserializer = AvroDeserializer(self._sr, json.dumps(VALUE_SCHEMA))

        self._consumer = Consumer({
            'bootstrap.servers': config.kafka_brokers,
            'group.id': config.consumer_group,
            'security.protocol': config.security_protocol,
            'sasl.mechanism': config.sasl_mechanism,
            'sasl.username': config.kafka_user,
            'sasl.password': config.kafka_password,
            'ssl.ca.location': config.ca_file,
            'auto.offset.reset': 'earliest',
        })
        self._consumer.subscribe([config.kafka_topic])

    def run(self) -> None:
        log.info("Avro consumer started, waiting for messages...")
        while True:
            msg = self._consumer.poll(10)
            if msg is None:
                continue
            if msg.error():
                log.error("Consumer error: %s", msg.error())
                continue

            try:
                key = self._key_deserializer(
                    msg.key(), SerializationContext(msg.topic(), MessageField.KEY))
                value = self._value_deserializer(
                    msg.value(), SerializationContext(msg.topic(), MessageField.VALUE))
            except SerializationError as e:
                log.warning("Skipping non-Avro message: %s", e)
                continue

            log.info("key=%s value=%s", key, value)

    def close(self) -> None:
        self._consumer.close()


if __name__ == "__main__":
    consumer = AvroConsumer()
    try:
        consumer.run()
    except KeyboardInterrupt:
        log.info("Interrupted by user")
    finally:
        consumer.close()
