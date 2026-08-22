"""Конфигурация инфраструктуры (брокеры, учётные данные, Schema Registry).

Отделяет инфраструктурные настройки от бизнес-логики: все значения
загружаются из переменных окружения (файл `.env` через python-dotenv),
а доступ осуществляется через единый объект `config` в стиле dict
(`config.get(...)`, `config["..."]`, `config.kafka_brokers`).
"""

from typing import Any, Dict

from dotenv import load_dotenv

load_dotenv()


class Config:
    """Словарь настроек с удобным доступом key=value.

    Значения по умолчанию совпадают с локальным окружением разработки.
    В production значения переопределяются через .env / переменные окружения.
    """

    _defaults: Dict[str, Any] = {
        "kafka_brokers": "rc1a-6ibie76edoio2ab7.mdb.yandexcloud.net:9091",
        "kafka_topic": "topic-1",
        "kafka_user": "practicumuser",
        "kafka_password": "SecurePass2026",
        "schema_registry_url": "http://localhost:8081",
        "ca_file": "YandexInternalRootCA.crt",
        "consumer_group": "avro-consumer",
        "security_protocol": "SASL_SSL",
        "sasl_mechanism": "SCRAM-SHA-512",
    }

    def __init__(self, defaults: Dict[str, Any] | None = None) -> None:
        self._data: Dict[str, Any] = {}
        merged = dict(self._defaults)
        if defaults:
            merged.update(defaults)
        for key, default in merged.items():
            self._data[key] = self._read(key, default)

    @staticmethod
    def _read(key: str, default: Any) -> Any:
        return __import__("os").getenv(key.upper(), default)

    def get(self, key: str, default: Any = None) -> Any:
        """Доступ в стиле dict: config.get('kafka_brokers')."""
        return self._data.get(key, default)

    def __getitem__(self, key: str) -> Any:
        return self._data[key]

    def __getattr__(self, name: str) -> Any:
        if name in self._data:
            return self._data[name]
        raise AttributeError(f"Unknown config key: {name}")

    def as_dict(self) -> Dict[str, Any]:
        return dict(self._data)


config = Config()
