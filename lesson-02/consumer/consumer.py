import json
import time
import logging

from kafka import KafkaConsumer

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

time.sleep(25)

consumer = KafkaConsumer(
    "filtered_messages",
    bootstrap_servers="kafka1:9092",
    auto_offset_reset="earliest",
    group_id="result-consumer",
    value_deserializer=lambda x: json.loads(x.decode("utf-8")),
)

logger.info("Waiting for filtered messages...")

for message in consumer:

    logger.info("\n===== RECEIVED MESSAGE =====")
    logger.info(json.dumps(message.value, ensure_ascii=False, indent=4))
