"""Логгер-фасад: единая точка настройки вывода в консоль.

Скрывает детали стандартного модуля logging за простым интерфейсом,
чтобы любой компонент получал настроенный логгер одним вызовом
``get_logger(__name__)`` без дублирования конфигурации хендлеров/формата.
"""

import logging

_DEFAULT_FORMAT = "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"
_configured = False


def configure(level: int = logging.INFO, fmt: str = _DEFAULT_FORMAT) -> None:
    """Идемпотентно настраивает корневой логгер на вывод в консоль."""
    global _configured
    if _configured:
        return

    root = logging.getLogger()
    root.setLevel(level)

    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter(fmt))
    root.addHandler(handler)

    # httpx/urllib3 шумят DEBUG/INFO-логами о каждом HTTP-запросе к Schema Registry;
    # поднимаем уровень, чтобы не засорять консоль приложения.
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)

    _configured = True


def get_logger(name: str) -> logging.Logger:
    """Возвращает логгер с именем ``name``, готовый к использованию."""
    configure()
    return logging.getLogger(name)
