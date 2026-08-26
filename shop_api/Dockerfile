FROM python:3.11-slim

WORKDIR /app

COPY shop_api/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY shop_api/producer.py .
COPY data/products.json /app/data/products.json

CMD ["python", "-u", "producer.py"]
