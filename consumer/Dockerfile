FROM python:3.11-slim

WORKDIR /app

COPY consumer/consumer.py .

RUN pip install kafka-python


CMD ["python", "consumer.py"]