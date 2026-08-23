import json
import os

from kafka import KafkaConsumer

KAFKA_HOST = os.getenv("KAFKA_HOST", "rc1a-6ibie76edoio2ab7.mdb.yandexcloud.net")
KAFKA_PORT = int(os.getenv("KAFKA_PORT", "9091"))
KAFKA_USER = os.getenv("KAFKA_USER", "practicumuser")
KAFKA_PASSWORD = os.getenv("KAFKA_PASSWORD", "SecurePass2026")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "topic-1")
CA_FILE = os.getenv("CA_FILE", "YandexInternalRootCA.crt")


def main() -> None:
    consumer = KafkaConsumer(
        bootstrap_servers=f"{KAFKA_HOST}:{KAFKA_PORT}",
        security_protocol="SASL_SSL",
        sasl_mechanism="SCRAM-SHA-512",
        sasl_plain_username=KAFKA_USER,
        sasl_plain_password=KAFKA_PASSWORD,
        ssl_cafile=CA_FILE,
        api_version=(2, 8, 0),
        auto_offset_reset="earliest",
        consumer_timeout_ms=10000,
    )
    consumer.subscribe([KAFKA_TOPIC])

    print("Consuming from '{}' (Ctrl-C to stop)...".format(KAFKA_TOPIC))
    for message in consumer:
        try:
            value = json.loads(message.value.decode("utf-8"))
        except (ValueError, AttributeError):
            value = message.value
        print(f"offset={message.offset} value={value}")

    consumer.close()


if __name__ == "__main__":
    main()
