FROM python:3.9-slim

WORKDIR /app

COPY processor/. .

RUN pip install --no-cache-dir -r requirements.txt

# Устанавливаем netcat для проверки портов
RUN apt-get update && apt-get install -y netcat-openbsd && rm -rf /var/lib/apt/lists/*

# Проверяем, что все файлы на месте
RUN ls -la /app/

EXPOSE 8000

CMD ["faust", "-A", "stream_processor", "worker", "-l", "info"]