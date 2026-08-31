FROM python:3.10-slim
WORKDIR /app
RUN pip install kafka-python
COPY consumer_batch/consumer_batch.py .
COPY common ./common
CMD ["python", "-u", "consumer_batch.py"]