import json
import os
import ssl
import time
import logging

from kafka import KafkaProducer

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Настройки из переменных окружения
BOOTSTRAP_SERVERS = os.getenv('BOOTSTRAP_SERVERS', 'kafka1:9092,kafka2:9093,kafka3:9094').split(',')
TOPIC = os.getenv('TOPIC', 'messages')
SEND_INTERVAL = float(os.getenv('SEND_INTERVAL', '2'))
SECURITY_PROTOCOL = os.getenv('SECURITY_PROTOCOL', 'PLAINTEXT')
SSL_CAFILE = os.getenv('SSL_CAFILE', '')
SSL_CERTFILE = os.getenv('SSL_CERTFILE', '')
SSL_KEYFILE = os.getenv('SSL_KEYFILE', '')
SSL_CHECK_HOSTNAME = os.getenv('SSL_CHECK_HOSTNAME', 'true').lower() == 'true'


def build_ssl_context():
    context = ssl.create_default_context(cafile=SSL_CAFILE)
    if SSL_CERTFILE and SSL_KEYFILE:
        context.load_cert_chain(certfile=SSL_CERTFILE, keyfile=SSL_KEYFILE)
    if not SSL_CHECK_HOSTNAME:
        context.check_hostname = False
    return context


def main():
    ssl_context = build_ssl_context() if SECURITY_PROTOCOL == 'SSL' else None
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8'),
        acks='all',
        retries=3,
        request_timeout_ms=30000,
        security_protocol=SECURITY_PROTOCOL,
        ssl_context=ssl_context,
    )

    message_id = 0
    try:
        while True:
            msg = {
                "id": message_id,
                "sender": f"user{message_id % 10}",
                "receiver": f"user{(message_id + 1) % 10}",
                "text": f"Hello Kafka {message_id}",
            }
            logger.info(f"Sending: {msg}")

            future = producer.send(TOPIC, value=msg)
            try:
                record_metadata = future.get(timeout=10)
                logger.debug(
                    f"Sent to topic {record_metadata.topic}, "
                    f"partition {record_metadata.partition}, offset {record_metadata.offset}"
                )
            except Exception as e:
                logger.error(f"Failed to send message: {e}")

            message_id += 1
            time.sleep(SEND_INTERVAL)
    except KeyboardInterrupt:
        logger.info("Shutting down producer")
    finally:
        producer.close()


if __name__ == "__main__":
    main()
