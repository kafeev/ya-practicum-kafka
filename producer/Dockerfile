FROM python:3.11-slim

WORKDIR /app

COPY producer/requirements.txt .
COPY producer/producer.py .

RUN pip install --no-cache-dir -r requirements.txt

CMD ["python", "-u", "producer.py"]
