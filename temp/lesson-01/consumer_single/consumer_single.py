import os
import logging
from kafka import KafkaConsumer
from common.message import Message

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

BOOTSTRAP_SERVERS = os.getenv('BOOTSTRAP_SERVERS', 'kafka1:9092,kafka2:9092').split(',')
TOPIC = os.getenv('TOPIC', 'my-topic')
GROUP_ID = os.getenv('GROUP_ID', 'single-consumer-group')

def main():
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id=GROUP_ID,
        # Автоматический коммит оффсетов (по умолчанию включён)
        enable_auto_commit=True,
        auto_commit_interval_ms=5000,
        # Десериализатор: байты -> строка
        value_deserializer=lambda v: v.decode('utf-8'),
        # Начинаем читать с самого последнего (чтобы не читать старые)
        auto_offset_reset='latest'
    )

    logger.info(f"Single consumer started, group_id={GROUP_ID}")
    try:
        for msg in consumer:
            try:
                # Десериализуем сообщение
                message = Message.from_json(msg.value)
                logger.info(f"Single consumer received: {message} (partition={msg.partition}, offset={msg.offset})")
                # Здесь могла бы быть бизнес-обработка
            except Exception as e:
                logger.error(f"Error processing message: {e}, raw value: {msg.value}")
                # Продолжаем работу, не прерывая поток
                continue
    except KeyboardInterrupt:
        logger.info("Shutting down single consumer")
    finally:
        consumer.close()

if __name__ == "__main__":
    main()