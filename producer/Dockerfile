FROM python:3.11-slim

WORKDIR /app

COPY producer/requirements.txt .
COPY producer/app.py .

RUN pip install --no-cache-dir -r requirements.txt

# Устанавливаем netcat
RUN apt-get update && apt-get install -y netcat-openbsd && rm -rf /var/lib/apt/lists/*

# Отладочная информация
RUN echo "Содержимое /app:" && ls -la /app/
RUN echo "Содержимое app.py:" && head -n 5 /app/app.py || echo "Файл app.py не найден!"

EXPOSE 8000

# Запускаем приложение
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8000", "--reload"]