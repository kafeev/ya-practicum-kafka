import json
import os

from kafka import KafkaProducer
from kafka.serializer import Serializer

KAFKA_HOST = os.getenv("KAFKA_HOST", "rc1a-6ibie76edoio2ab7.mdb.yandexcloud.net")
KAFKA_PORT = int(os.getenv("KAFKA_PORT", "9091"))
KAFKA_USER = os.getenv("KAFKA_USER", "practicumuser")
KAFKA_PASSWORD = os.getenv("KAFKA_PASSWORD", "SecurePass2026")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "topic-1")
CA_FILE = os.getenv("CA_FILE", "YandexInternalRootCA.crt")


class JsonSerializer(Serializer):
    def serialize(self, topic, value):
        return json.dumps(value).encode("utf-8")


def main() -> None:
    producer = KafkaProducer(
        bootstrap_servers=f"{KAFKA_HOST}:{KAFKA_PORT}",
        security_protocol="SASL_SSL",
        sasl_mechanism="SCRAM-SHA-512",
        sasl_plain_username=KAFKA_USER,
        sasl_plain_password=KAFKA_PASSWORD,
        ssl_cafile=CA_FILE,
        api_version=(2, 8, 0),
        value_serializer=JsonSerializer(),
    )

    producer.send(KAFKA_TOPIC, key=b"key-1", value={"message": "hello from producer"})
    producer.flush()
    print("Message sent")
    producer.close()


if __name__ == "__main__":
    main()