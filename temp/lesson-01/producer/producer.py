import os
import time
import logging
from kafka import KafkaProducer
from common.message import Message

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Настройки из переменных окружения
BOOTSTRAP_SERVERS = os.getenv('BOOTSTRAP_SERVERS', 'kafka1:9092,kafka2:9092').split(',')
TOPIC = os.getenv('TOPIC', 'my-topic')

def main():
    # Продюсер с гарантией At Least Once
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        # Сериализатор: преобразуем строку JSON в байты
        value_serializer=lambda v: v.encode('utf-8'),
        # acks=all — подтверждение от всех реплик
        acks='all',
        # retries — количество повторных попыток при временных ошибках
        retries=3,
        # Включение идемпотентности для избежания дублей (часть exactly-once, но совместно с retries даёт at-least-once)
        enable_idempotence=True,
        # Максимальное время ожидания ответа от брокера
        request_timeout_ms=30000
    )

    message_id = 0
    try:
        while True:
            # Создаём сообщение
            msg = Message(id=message_id, text=f"Hello Kafka {message_id}")
            serialized = msg.to_json()
            logger.info(f"Sending: {msg}")

            # Асинхронная отправка с callback для логирования
            future = producer.send(TOPIC, value=serialized)
            # Можно добавить синхронное ожидание для обработки ошибок
            try:
                record_metadata = future.get(timeout=10)
                logger.debug(f"Sent to partition {record_metadata.partition} offset {record_metadata.offset}")
            except Exception as e:
                logger.error(f"Failed to send message: {e}")

            message_id += 1
            time.sleep(2)  # Пауза между отправками
    except KeyboardInterrupt:
        logger.info("Shutting down producer")
    finally:
        producer.close()

if __name__ == "__main__":
    main()