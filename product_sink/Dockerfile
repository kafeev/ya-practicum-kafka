FROM python:3.11-slim

WORKDIR /app

COPY product_sink/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY product_sink/consumer.py .

CMD ["python", "-u", "consumer.py"]
