FROM python:3.11-slim

WORKDIR /app

COPY client_api/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY client_api/client_api.py .

CMD ["python", "-u", "client_api.py"]
