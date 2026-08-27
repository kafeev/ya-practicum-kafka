import json
import os
import ssl
import re
import time
import logging
from datetime import datetime, timezone

import psycopg2
from kafka import KafkaProducer

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
logger = logging.getLogger("client-api")

BOOTSTRAP_SERVERS = os.getenv("BOOTSTRAP_SERVERS", "kafka1:9095,kafka2:9096,kafka3:9097").split(",")
REQ_TOPIC = os.getenv("TOPIC", "client_requests")
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

STOPWORDS = {"и", "с", "в", "на", "по", "для", "от", "до", "к", "о", "а", "я", "за", "из", "под", "над"}


def now_iso():
    return datetime.now(timezone.utc).isoformat()


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


def send_request_event(producer, event):
    try:
        producer.send(REQ_TOPIC, value=event).get(timeout=10)
        producer.flush()
    except Exception as e:
        logger.error("Не удалось отправить событие в Kafka: %s", e)


def cmd_search(conn, producer, user_id, query):
    cur = conn.cursor()
    # Шаг 5 (расширенный вариант): полнотекстовый поиск по индексу search_vector
    # с ранжированием релевантности (ts_rank). При неудаче/пустом результате —
    # запасной подстроковый поиск (ILIKE) для частичных слов.
    rows = []
    try:
        cur.execute(
            """
            SELECT product_id, name, brand, category, price_amount, price_currency, stock_available
            FROM products, websearch_to_tsquery('russian', %s) q
            WHERE search_vector @@ q
            ORDER BY ts_rank(search_vector, q) DESC
            LIMIT 20
            """,
            (query,),
        )
        rows = cur.fetchall()
    except Exception as e:
        logger.warning("Полнотекстовый поиск не удался (%s), откат к ILIKE", e)

    if not rows:
        like = f"%{query}%"
        cur.execute(
            """
            SELECT product_id, name, brand, category, price_amount, price_currency, stock_available
            FROM products
            WHERE name ILIKE %s OR description ILIKE %s OR tags::text ILIKE %s
            ORDER BY name
            LIMIT 20
            """,
            (like, like, like),
        )
        rows = cur.fetchall()

    ids = [r[0] for r in rows]

    cur.execute(
        "INSERT INTO client_events(user_id, event_type, query, product_ids) VALUES (%s, 'search', %s, %s)",
        (user_id, query, ids),
    )
    conn.commit()

    send_request_event(producer, {"type": "search", "user_id": user_id, "query": query, "ts": now_iso()})

    if not rows:
        print(f"\nПо запросу '{query}' ничего не найдено.")
        return
    print(f"\nНайдено товаров: {len(rows)}")
    for pid, name, brand, cat, price, cur_, stock in rows:
        print(f"  [{pid}] {name} | {brand} | {cat} | {price} {cur_} | на складе: {stock}")


def cmd_recommend(conn, producer, user_id):
    send_request_event(producer, {"type": "recommend", "user_id": user_id, "ts": now_iso()})

    cur = conn.cursor()
    cur.execute(
        "SELECT query, product_ids FROM client_events WHERE user_id=%s AND event_type='search' ORDER BY id DESC LIMIT 20",
        (user_id,),
    )
    history = cur.fetchall()

    keywords = set()
    viewed = set()
    for q, pids in history:
        if q:
            for w in re.findall(r"[а-яёa-z0-9]+", q.lower()):
                if len(w) > 2 and w not in STOPWORDS:
                    keywords.add(w)
        if pids:
            viewed.update(pids)

    recs = []
    cur.execute("SELECT product_id, name, brand, category, price_amount, price_currency, tags, description FROM products")
    for pid, name, brand, cat, price, cur_, tags, desc in cur.fetchall():
        if pid in viewed:
            continue
        text = " ".join(str(x) for x in (name, brand, cat, desc, " ".join(tags or []))).lower()
        score = sum(1 for kw in keywords if kw in text)
        if score > 0:
            recs.append((score, pid, name, brand, cat, price, cur_))

    recs.sort(key=lambda x: (-x[0], x[2]))
    source = "на основе ваших поисков"
    if not recs:
        source = "популярное (история поиска пуста)"
        cur.execute(
            "SELECT product_id, name, brand, category, price_amount, price_currency FROM products "
            "ORDER BY updated_at DESC NULLS LAST LIMIT 5"
        )
        recs = [(0,) + tuple(r) for r in cur.fetchall()]

    rec_ids = [r[1] for r in recs[:10]]
    cur.execute(
        "INSERT INTO client_events(user_id, event_type, query, product_ids) VALUES (%s, 'recommend', NULL, %s)",
        (user_id, rec_ids),
    )
    conn.commit()

    print(f"\nПерсональные рекомендации для {user_id} ({source}):")
    if not recs:
        print("  В каталоге пока нет товаров.")
        return
    for score, pid, name, brand, cat, price, cur_ in recs[:10]:
        tag = f" (совпадений: {score})" if score else ""
        print(f"  [{pid}] {name} | {brand} | {cat} | {price} {cur_}{tag}")


def print_help():
    print(
        "\nКоманды CLIENT API:\n"
        "  search <user_id> <запрос>   поиск товара по имени/описанию\n"
        "  recommend <user_id>          персональные рекомендации\n"
        "  help                         показать справку\n"
        "  exit / quit                  выход\n"
        "Примеры:\n"
        "  search user_1 часы\n"
        "  search user_1 смартфон ABC\n"
        "  recommend user_1\n"
    )


def main():
    conn = connect_db()
    ssl_context = build_ssl_context()
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
        acks="all",
        retries=5,
        security_protocol=SECURITY_PROTOCOL,
        ssl_context=ssl_context,
    )
    logger.info("CLIENT API готов. Топик запросов='%s'", REQ_TOPIC)
    print_help()

    while True:
        try:
            line = input("client> ").strip()
        except EOFError:
            break
        except KeyboardInterrupt:
            print()
            break
        if not line:
            continue

        parts = line.split(None, 2)
        cmd = parts[0].lower()

        if cmd in ("exit", "quit"):
            break
        elif cmd == "help":
            print_help()
        elif cmd == "search":
            if len(parts) < 3:
                print("Использование: search <user_id> <запрос>")
                continue
            try:
                cmd_search(conn, producer, parts[1], parts[2])
            except psycopg2.OperationalError:
                logger.error("Соединение с БД потеряно. Переподключение...")
                try:
                    conn.close()
                except Exception:
                    pass
                conn = connect_db()
        elif cmd == "recommend":
            if len(parts) < 2:
                print("Использование: recommend <user_id>")
                continue
            try:
                cmd_recommend(conn, producer, parts[1])
            except psycopg2.OperationalError:
                logger.error("Соединение с БД потеряно. Переподключение...")
                try:
                    conn.close()
                except Exception:
                    pass
                conn = connect_db()
        else:
            print(f"Неизвестная команда: {cmd}. Введите 'help'.")

    producer.close()
    try:
        conn.close()
    except Exception:
        pass
    print("CLIENT API завершён.")


if __name__ == "__main__":
    main()
