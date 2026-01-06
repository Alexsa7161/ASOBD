FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY rabbit_consumer.py .
CMD ["python", "rabbit_consumer.py"]
