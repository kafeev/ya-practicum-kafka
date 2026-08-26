import json
import os
import ssl
import time
import logging

from kafka import KafkaProducer

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
logger = logging.getLogger("shop-api")

BOOTSTRAP_SERVERS = os.getenv("BOOTSTRAP_SERVERS", "kafka1:9095,kafka2:9096,kafka3:9097").split(",")
TOPIC = os.getenv("TOPIC", "products")
SEND_INTERVAL = float(os.getenv("SEND_INTERVAL", "60"))
DATA_FILE = os.getenv("DATA_FILE", "/app/data/products.json")
SECURITY_PROTOCOL = os.getenv("SECURITY_PROTOCOL", "SSL")
SSL_CAFILE = os.getenv("SSL_CAFILE", "")
SSL_CERTFILE = os.getenv("SSL_CERTFILE", "")
SSL_KEYFILE = os.getenv("SSL_KEYFILE", "")
SSL_CHECK_HOSTNAME = os.getenv("SSL_CHECK_HOSTNAME", "false").lower() == "true"


def build_ssl_context():
    context = ssl.create_default_context(cafile=SSL_CAFILE)
    context.load_cert_chain(certfile=SSL_CERTFILE, keyfile=SSL_KEYFILE)
    if not SSL_CHECK_HOSTNAME:
        context.check_hostname = False
    return context


def main():
    ssl_context = build_ssl_context()
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
        acks="all",
        retries=5,
        request_timeout_ms=30000,
        security_protocol=SECURITY_PROTOCOL,
        ssl_context=ssl_context,
    )
    logger.info("SHOP API готов. Топик=%s, файл=%s", TOPIC, DATA_FILE)

    while True:
        try:
            with open(DATA_FILE, encoding="utf-8") as f:
                products = json.load(f)
        except Exception as e:
            logger.error("Не удалось прочитать файл '%s': %s", DATA_FILE, e)
            time.sleep(SEND_INTERVAL)
            continue

        for p in products:
            producer.send(TOPIC, value=p)
            logger.info("Отправлен товар %s (%s) — store_id=%s", p.get("product_id"), p.get("name"), p.get("store_id"))
        producer.flush()
        logger.info("В топик '%s' отправлено %d товаров. Пауза %ss", TOPIC, len(products), SEND_INTERVAL)
        time.sleep(SEND_INTERVAL)


if __name__ == "__main__":
    main()
