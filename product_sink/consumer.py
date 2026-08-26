import json
import os
import ssl
import time
import logging

import psycopg2
from kafka import KafkaConsumer

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
logger = logging.getLogger("product-sink")

BOOTSTRAP_SERVERS = os.getenv("BOOTSTRAP_SERVERS", "kafka1:9095,kafka2:9096,kafka3:9097").split(",")
TOPIC = os.getenv("TOPIC", "products")
GROUP_ID = os.getenv("GROUP_ID", "product-sink-group")
SECURITY_PROTOCOL = os.getenv("SECURITY_PROTOCOL", "SSL")
SSL_CAFILE = os.getenv("SSL_CAFILE", "")
SSL_CERTFILE = os.getenv("SSL_CERTFILE", "")
SSL_KEYFILE = os.getenv("SSL_KEYFILE", "")
SSL_CHECK_HOSTNAME = os.getenv("SSL_CHECK_HOSTNAME", "false").lower() == "true"

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DB = os.getenv("POSTGRES_DB", "marketplace")
POSTGRES_USER = os.getenv("POSTGRES_USER", "marketplace")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "marketplace")

UPSERT_SQL = """
INSERT INTO products (
    product_id, name, description, price_amount, price_currency, category, brand,
    stock_available, stock_reserved, sku, tags, images, specifications,
    created_at, updated_at, index, store_id
) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (product_id) DO UPDATE SET
    name=EXCLUDED.name, description=EXCLUDED.description,
    price_amount=EXCLUDED.price_amount, price_currency=EXCLUDED.price_currency,
    category=EXCLUDED.category, brand=EXCLUDED.brand,
    stock_available=EXCLUDED.stock_available, stock_reserved=EXCLUDED.stock_reserved,
    sku=EXCLUDED.sku, tags=EXCLUDED.tags, images=EXCLUDED.images,
    specifications=EXCLUDED.specifications, created_at=EXCLUDED.created_at,
    updated_at=EXCLUDED.updated_at, index=EXCLUDED.index, store_id=EXCLUDED.store_id
"""


def build_ssl_context():
    context = ssl.create_default_context(cafile=SSL_CAFILE)
    context.load_cert_chain(certfile=SSL_CERTFILE, keyfile=SSL_KEYFILE)
    if not SSL_CHECK_HOSTNAME:
        context.check_hostname = False
    return context


def connect_db():
    while True:
        try:
            conn = psycopg2.connect(
                host=POSTGRES_HOST, port=POSTGRES_PORT, dbname=POSTGRES_DB,
                user=POSTGRES_USER, password=POSTGRES_PASSWORD, connect_timeout=10,
            )
            conn.autocommit = False
            logger.info("Подключено к PostgreSQL (%s:%s/%s)", POSTGRES_HOST, POSTGRES_PORT, POSTGRES_DB)
            return conn
        except Exception as e:
            logger.warning("PostgreSQL недоступен: %s. Повтор через 5с", e)
            time.sleep(5)


def deserialize(value):
    try:
        return json.loads(value.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        logger.warning("Пропущено не-JSON сообщение: %r", value)
        return None


def upsert(conn, p):
    price = p.get("price") or {}
    stock = p.get("stock") or {}
    with conn.cursor() as cur:
        cur.execute(UPSERT_SQL, (
            p.get("product_id"),
            p.get("name"),
            p.get("description"),
            price.get("amount"),
            price.get("currency"),
            p.get("category"),
            p.get("brand"),
            stock.get("available"),
            stock.get("reserved"),
            p.get("sku"),
            json.dumps(p.get("tags")) if p.get("tags") is not None else None,
            json.dumps(p.get("images")) if p.get("images") is not None else None,
            json.dumps(p.get("specifications")) if p.get("specifications") is not None else None,
            p.get("created_at"),
            p.get("updated_at"),
            p.get("index"),
            p.get("store_id"),
        ))
    conn.commit()


def main():
    conn = connect_db()
    ssl_context = build_ssl_context()
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id=GROUP_ID,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=deserialize,
        security_protocol=SECURITY_PROTOCOL,
        ssl_context=ssl_context,
    )
    logger.info("product-sink слушает топик '%s' (group=%s)", TOPIC, GROUP_ID)

    for message in consumer:
        p = message.value
        if p is None:
            continue
        try:
            upsert(conn, p)
            logger.info("Сохранён товар %s (%s)", p.get("product_id"), p.get("name"))
        except psycopg2.OperationalError as e:
            logger.error("Соединение с БД потеряно (%s). Переподключение...", e)
            try:
                conn.close()
            except Exception:
                pass
            conn = connect_db()
        except Exception as e:
            logger.error("Ошибка сохранения товара %s: %s", p.get("product_id"), e)
            try:
                conn.rollback()
            except Exception:
                pass


if __name__ == "__main__":
    main()
