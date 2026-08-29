import json
import os
import time

from kafka import KafkaProducer

KAFKA_HOST = os.getenv("KAFKA_HOST", "rc1a-6ibie76edoio2ab7.mdb.yandexcloud.net")
KAFKA_PORT = int(os.getenv("KAFKA_PORT", "9091"))
KAFKA_USER = os.getenv("KAFKA_USER", "practicumuser")
KAFKA_PASSWORD = os.getenv("KAFKA_PASSWORD", "SecurePass2026")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "topic-1")
CA_FILE = os.getenv("CA_FILE", "YandexInternalRootCA.crt")


def main() -> None:
    producer = KafkaProducer(
        bootstrap_servers=f"{KAFKA_HOST}:{KAFKA_PORT}",
        security_protocol="SASL_SSL",
        sasl_mechanism="SCRAM-SHA-512",
        sasl_plain_username=KAFKA_USER,
        sasl_plain_password=KAFKA_PASSWORD,
        ssl_cafile=CA_FILE,
        api_version=(2, 8, 0),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    for i in range(5):
        payload = {
            "source": "nifi-integration-demo",
            "message": f"event-{i}",
            "ts": int(time.time()),
        }
        producer.send(KAFKA_TOPIC, key=b"nifi", value=payload)
        print(f"sent {payload}")

    producer.flush()
    print("All messages sent to Kafka topic '{}'".format(KAFKA_TOPIC))
    producer.close()


if __name__ == "__main__":
    main()
