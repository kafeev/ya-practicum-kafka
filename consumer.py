import json
import os
import sys
import time

from kafka import KafkaConsumer
from kafka.errors import KafkaError

BOOTSTRAP_SERVERS = os.getenv("BOOTSTRAP_SERVERS", "localhost:9092").split(",")
TOPICS = os.getenv("TOPICS", "pg-server.public.users,pg-server.public.orders").split(",")
RETRY_INTERVAL = int(os.getenv("RETRY_INTERVAL", "5"))


def create_consumer():
    return KafkaConsumer(
        *TOPICS,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        key_deserializer=lambda v: v.decode("utf-8") if v else None,
    )


def main():
    print(f"Подключение к {BOOTSTRAP_SERVERS}. Топики: {TOPICS}")

    consumer = None
    while True:
        try:
            if consumer is None:
                consumer = create_consumer()
                print("Подключено к Kafka. Начинаем чтение...\n")
            for message in consumer:
                payload = message.value.get("payload", {}) if isinstance(message.value, dict) else {}
                op = payload.get("op")
                after = payload.get("after")
                before = payload.get("before")

                print(
                    f"[{message.topic}] "
                    f"partition={message.partition} offset={message.offset} "
                    f"op={op}"
                )
                if op == "c":
                    print("  CREATE:", json.dumps(after, ensure_ascii=False))
                elif op == "u":
                    print("  UPDATE before:", json.dumps(before, ensure_ascii=False))
                    print("         after :", json.dumps(after, ensure_ascii=False))
                elif op == "d":
                    print("  DELETE:", json.dumps(before, ensure_ascii=False))
                elif op == "r":
                    print("  READ (snapshot):", json.dumps(after, ensure_ascii=False))
                else:
                    print("  RAW:", json.dumps(message.value, ensure_ascii=False))
                print("-" * 60)
                sys.stdout.flush()
        except KeyboardInterrupt:
            print("\nОстановка consumer...")
            break
        except KafkaError as e:
            print(f"Kafka ошибка: {e}", file=sys.stderr)
            if consumer is not None:
                consumer.close()
            consumer = None
        except Exception as e:  # noqa: BLE001
            print(f"Ошибка: {e}", file=sys.stderr)
            if consumer is not None:
                consumer.close()
            consumer = None

        if consumer is None:
            print(f"Повторное подключение через {RETRY_INTERVAL}с...\n")
            sys.stdout.flush()
            time.sleep(RETRY_INTERVAL)

    if consumer is not None:
        consumer.close()


if __name__ == "__main__":
    main()
