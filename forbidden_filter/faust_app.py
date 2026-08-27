import os
import ssl
import logging

from faust import App
from faust.auth import SSLCredentials

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
logger = logging.getLogger("forbidden-filter")

BOOTSTRAP = os.getenv("BOOTSTRAP_SERVERS", "kafka1:9095,kafka2:9096,kafka3:9097")
SSL_CAFILE = os.getenv("SSL_CAFILE", "/etc/kafka/secrets/ca-cert")
SSL_CERTFILE = os.getenv("SSL_CERTFILE", "/etc/kafka/secrets/client.producer-cert.pem")
SSL_KEYFILE = os.getenv("SSL_KEYFILE", "/etc/kafka/secrets/client.producer-key.pem")
SSL_CHECK_HOSTNAME = os.getenv("SSL_CHECK_HOSTNAME", "false").lower() == "true"

PRODUCTS_TOPIC = os.getenv("PRODUCTS_TOPIC", "products")
FORBIDDEN_TOPIC = os.getenv("FORBIDDEN_TOPIC", "forbidden-products")
ALLOWED_TOPIC = os.getenv("ALLOWED_TOPIC", "products-allowed")
REJECTED_TOPIC = os.getenv("REJECTED_TOPIC", "products-rejected")

# Состояние: множество запрещённых product_id.
# Источник истины — компактный топик FORBIDDEN_TOPIC; агент manage_forbidden
# проигрывает его с начала (earliest) при старте и держит словарь в актуальном
# состоянии (добавление/удаление через CLI публикует события в тот же топик).
FORBIDDEN = {}


def build_ssl_context():
    ctx = ssl.create_default_context(cafile=SSL_CAFILE)
    ctx.load_cert_chain(certfile=SSL_CERTFILE, keyfile=SSL_KEYFILE)
    ctx.check_hostname = SSL_CHECK_HOSTNAME
    return ctx


def make_app():
    ctx = build_ssl_context()
    # aiokafka ожидает список брокеров (list), иначе при парсинге
    # строки с несколькими хостами потребитель откатывается на 127.0.0.1:9092
    broker = [f"kafka://{b}" for b in BOOTSTRAP.split(",") if b]
    return App(
        "forbidden-filter",
        broker=broker,
        broker_credentials=SSLCredentials(ctx),
        value_serializer="json",
        topic_replication_factor=3,
        broker_request_timeout=90,
    )


app = make_app()

products_in = app.topic(PRODUCTS_TOPIC)
forbidden_in = app.topic(FORBIDDEN_TOPIC)
allowed_out = app.topic(ALLOWED_TOPIC)
rejected_out = app.topic(REJECTED_TOPIC)


@app.agent(forbidden_in)
async def manage_forbidden(stream):
    """Поддерживает актуальным список запрещённых товаров.

    Сообщение со значением (value) — добавление/обновление записи,
    tombstone (value=None) — удаление из списка.
    """
    async for event in stream.events():
        key = event.key
        if isinstance(key, bytes):
            key = key.decode("utf-8")
        value = event.value
        if value is None:
            FORBIDDEN.pop(key, None)
            logger.info("Удалён из запрещённых: %s", key)
        else:
            pid = str(value.get("product_id"))
            FORBIDDEN[pid] = value
            logger.info("Добавлен в запрещённые: %s (%s)", pid, value.get("name"))


@app.agent(products_in)
async def filter_products(stream):
    """Извлекает товары из топика магазинов и отфильтровывает запрещённые."""
    async for product in stream:
        if not isinstance(product, dict):
            continue
        pid = str(product.get("product_id"))
        name = product.get("name")
        if pid in FORBIDDEN:
            await rejected_out.send(key=pid, value=product)
            logger.info("ОТКЛОНЁН товар %s (%s) — в списке запрещённых", pid, name)
        else:
            await allowed_out.send(key=pid, value=product)
            logger.info("ПРОПУЩЕН товар %s (%s)", pid, name)


if __name__ == "__main__":
    app.main()
