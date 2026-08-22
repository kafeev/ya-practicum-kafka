import json
import os

from kafka import KafkaConsumer, TopicPartition
from kafka.serializer import Deserializer

KAFKA_HOST = os.getenv("KAFKA_HOST", "rc1a-6ibie76edoio2ab7.mdb.yandexcloud.net")
KAFKA_PORT = int(os.getenv("KAFKA_PORT", "9091"))
KAFKA_USER = os.getenv("KAFKA_USER", "practicumuser")
KAFKA_PASSWORD = os.getenv("KAFKA_PASSWORD", "SecurePass2026")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "topic-1")
CA_FILE = os.getenv("CA_FILE", "YandexInternalRootCA.crt")


class JsonDeserializer(Deserializer):
    def deserialize(self, topic, value):
        try:
            return json.loads(value.decode("utf-8"))
        except (ValueError, AttributeError):
            # пропускаем сообщения, не являющиеся JSON
            # (например, Avro-сообщения из avro_producer.py)
            return None


def main() -> None:
    consumer = KafkaConsumer(
        bootstrap_servers=f"{KAFKA_HOST}:{KAFKA_PORT}",
        security_protocol="SASL_SSL",
        sasl_mechanism="SCRAM-SHA-512",
        sasl_plain_username=KAFKA_USER,
        sasl_plain_password=KAFKA_PASSWORD,
        ssl_cafile=CA_FILE,
        api_version=(2, 8, 0),
        value_deserializer=JsonDeserializer(),
        auto_offset_reset="earliest",
    )

    partitions = [
        TopicPartition(KAFKA_TOPIC, p)
        for p in consumer.partitions_for_topic(KAFKA_TOPIC)
    ]
    consumer.assign(partitions)
    consumer.seek_to_beginning(*partitions)

    while True:
        records = consumer.poll(timeout_ms=1000)
        for tp, messages in records.items():
            for message in messages:
                if message.value is None:
                    continue
                print(f"offset={message.offset} value={message.value}")


if __name__ == "__main__":
    main()