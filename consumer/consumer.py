import json
import os
import ssl
import time
import logging

from kafka import KafkaConsumer

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Настройки из переменных окружения
BOOTSTRAP_SERVERS = os.getenv('BOOTSTRAP_SERVERS', 'kafka1:9092,kafka2:9093,kafka3:9094').split(',')
TOPIC = os.getenv('TOPIC', 'messages')
GROUP_ID = os.getenv('GROUP_ID', 'ssl-consumer-group')
SECURITY_PROTOCOL = os.getenv('SECURITY_PROTOCOL', 'PLAINTEXT')
SSL_CAFILE = os.getenv('SSL_CAFILE', '')
SSL_CERTFILE = os.getenv('SSL_CERTFILE', '')
SSL_KEYFILE = os.getenv('SSL_KEYFILE', '')
SSL_CHECK_HOSTNAME = os.getenv('SSL_CHECK_HOSTNAME', 'true').lower() == 'true'
STARTUP_DELAY = float(os.getenv('STARTUP_DELAY', '0'))


def build_ssl_context():
    context = ssl.create_default_context(cafile=SSL_CAFILE)
    if SSL_CERTFILE and SSL_KEYFILE:
        context.load_cert_chain(certfile=SSL_CERTFILE, keyfile=SSL_KEYFILE)
    if not SSL_CHECK_HOSTNAME:
        context.check_hostname = False
    return context


ssl_context = build_ssl_context() if SECURITY_PROTOCOL == 'SSL' else None

time.sleep(STARTUP_DELAY)

def deserialize(value):
    try:
        return json.loads(value.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        logger.warning(f"Skipping non-JSON message: {value!r}")
        return None


consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=BOOTSTRAP_SERVERS,
    auto_offset_reset="earliest",
    group_id=GROUP_ID,
    value_deserializer=deserialize,
    security_protocol=SECURITY_PROTOCOL,
    ssl_context=ssl_context,
)

logger.info(f"Waiting for messages in topic '{TOPIC}'...")

for message in consumer:
    if message.value is None:
        continue

    logger.info("\n===== RECEIVED MESSAGE =====")
    logger.info(json.dumps(message.value, ensure_ascii=False, indent=4))
