import os
import ssl
import sys
import json
import argparse
import logging

from kafka import KafkaProducer, KafkaConsumer

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
logger = logging.getLogger("forbidden-cli")

BOOTSTRAP = os.getenv("BOOTSTRAP_SERVERS", "kafka1:9095,kafka2:9096,kafka3:9097").split(",")
SSL_CAFILE = os.getenv("SSL_CAFILE", "/etc/kafka/secrets/ca-cert")
SSL_CERTFILE = os.getenv("SSL_CERTFILE", "/etc/kafka/secrets/client.producer-cert.pem")
SSL_KEYFILE = os.getenv("SSL_KEYFILE", "/etc/kafka/secrets/client.producer-key.pem")
SSL_CHECK_HOSTNAME = os.getenv("SSL_CHECK_HOSTNAME", "false").lower() == "true"
FORBIDDEN_TOPIC = os.getenv("FORBIDDEN_TOPIC", "forbidden-products")


def build_ssl_context():
    ctx = ssl.create_default_context(cafile=SSL_CAFILE)
    ctx.load_cert_chain(certfile=SSL_CERTFILE, keyfile=SSL_KEYFILE)
    ctx.check_hostname = SSL_CHECK_HOSTNAME
    return ctx


def producer():
    return KafkaProducer(
        bootstrap_servers=BOOTSTRAP,
        security_protocol="SSL",
        ssl_context=build_ssl_context(),
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8") if v is not None else None,
        key_serializer=lambda k: str(k).encode("utf-8"),
    )


def cmd_add(args):
    item = {
        "product_id": str(args.product_id),
        "name": args.name,
        "reason": args.reason,
    }
    p = producer()
    p.send(FORBIDDEN_TOPIC, key=args.product_id, value=item)
    p.flush()
    print(f"Добавлено в запрещённые: {args.product_id} ({args.name}) — {args.reason}")


def cmd_remove(args):
    p = producer()
    # tombstone: value=None помечает удаление в компактном топике
    p.send(FORBIDDEN_TOPIC, key=args.product_id, value=None)
    p.flush()
    print(f"Удалено из запрещённых: {args.product_id}")


def cmd_list(args):
    ctx = build_ssl_context()
    consumer = KafkaConsumer(
        FORBIDDEN_TOPIC,
        bootstrap_servers=BOOTSTRAP,
        group_id="forbidden-cli-list",
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        security_protocol="SSL",
        ssl_context=ctx,
        consumer_timeout_ms=5000,
    )
    items = {}
    for msg in consumer:
        key = msg.key.decode("utf-8") if isinstance(msg.key, bytes) else msg.key
        if msg.value is None:
            items.pop(key, None)
        else:
            items[key] = json.loads(msg.value.decode("utf-8"))
    consumer.close()
    if not items:
        print("Список запрещённых товаров пуст.")
        return
    print(f"Запрещённые товары ({len(items)}):")
    for it in items.values():
        print(f"  - {it.get('product_id')}: {it.get('name')} — {it.get('reason')}")


def main():
    parser = argparse.ArgumentParser(
        description="Управление списком запрещённых товаров (Шаг 4)."
    )
    sub = parser.add_subparsers(dest="command", required=True)

    p_add = sub.add_parser("add", help="добавить товар в запрещённые")
    p_add.add_argument("product_id", help="идентификатор товара")
    p_add.add_argument("--name", default="", help="название товара")
    p_add.add_argument("--reason", default="", help="причина запрета")
    p_add.set_defaults(func=cmd_add)

    p_rm = sub.add_parser("remove", help="удалить товар из запрещённых")
    p_rm.add_argument("product_id", help="идентификатор товара")
    p_rm.set_defaults(func=cmd_remove)

    p_list = sub.add_parser("list", help="показать текущий список запрещённых")
    p_list.set_defaults(func=cmd_list)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
