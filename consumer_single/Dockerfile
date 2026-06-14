FROM python:3.10-slim
WORKDIR /app
RUN pip install kafka-python
COPY consumer_single/consumer_single.py .
COPY common ./common
CMD ["python", "-u", "consumer_single.py"]