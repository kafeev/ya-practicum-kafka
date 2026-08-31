from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from kafka import KafkaProducer
import json
import logging
from typing import Optional

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# Модель данных для входящего сообщения
class Message(BaseModel):
    sender: str = Field(..., min_length=1, description="Отправитель сообщения")
    receiver: str = Field(..., min_length=1, description="Получатель сообщения")
    text: str = Field(..., min_length=1, description="Текст сообщения")

    class Config:
        json_schema_extra = {
            "example": {
                "sender": "user123",
                "receiver": "user456",
                "text": "Привет, как дела?",
            }
        }


# Создание приложения FastAPI
app = FastAPI(
    title="Kafka Message Service",
    description="Сервис для приема сообщений по HTTP API и отправки в Kafka",
    version="1.0.0",
)

# Инициализация Kafka producer
try:
    producer = KafkaProducer(
        bootstrap_servers="kafka1:9092",
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda v: v.encode("utf-8") if v else None,
        acks="all",  # Подтверждение от всех реплик
        retries=3,
        max_in_flight_requests_per_connection=1,
    )
    logger.info("Успешное подключение к Kafka")
    logger.info("Добавление запрещенных слов...")

    producer.send(
        "banned_words",
        {
            "word": "идиот"
        }
    )

    producer.send(
        "banned_words",
        {
            "word": "дурак"
        }
    )

    logger.info("Блокировка пользователя Bob для Alice...")

    producer.send(
        "blocked_users",
        {
            "user": "alice",
            "blocked_user": "bob"
        }
    )
except Exception as e:
    logger.error(f"Ошибка подключения к Kafka: {e}")
    producer = None


@app.on_event("shutdown")
async def shutdown_event():
    """Закрытие соединения с Kafka при завершении работы"""
    if producer:
        producer.close()
        logger.info("Соединение с Kafka закрыто")


@app.get("/")
async def root():
    """Корневой эндпоинт для проверки работоспособности"""
    return {
        "service": "Kafka Message Service",
        "status": "running",
        "kafka_connected": producer is not None,
    }


@app.get("/health")
async def health_check():
    """Эндпоинт для проверки здоровья сервиса"""
    return {"status": "healthy", "kafka_connected": producer is not None}


@app.post("/send")
async def send_message(message: Message):
    """
    Принимает сообщение и отправляет его в Kafka топик 'messages'
    """
    if producer is None:
        raise HTTPException(status_code=503, detail="Kafka producer не инициализирован")

    try:
        # Преобразуем сообщение в словарь
        message_dict = message.model_dump()

        # Отправляем сообщение в Kafka
        future = producer.send(
            topic="messages",
            value=message_dict,
            key=message.sender,  # Используем отправителя как ключ для партиционирования
        )

        # Ждем подтверждения
        record_metadata = future.get(timeout=10)

        logger.info(
            f"Сообщение отправлено в Kafka. Топик: {record_metadata.topic}, "
            f"Партиция: {record_metadata.partition}, "
            f"Оффсет: {record_metadata.offset}"
        )

        return {
            "status": "success",
            "message": "Сообщение успешно отправлено в Kafka",
            "details": {
                "topic": record_metadata.topic,
                "partition": record_metadata.partition,
                "offset": record_metadata.offset,
            },
        }

    except Exception as e:
        logger.error(f"Ошибка отправки сообщения в Kafka: {e}")
        raise HTTPException(
            status_code=500, detail=f"Ошибка отправки сообщения в Kafka: {str(e)}"
        )
