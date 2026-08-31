FROM python:3.10-slim
WORKDIR /app
RUN pip install kafka-python
COPY producer/producer.py .
COPY common ./common
CMD ["python", "-u", "producer.py"]