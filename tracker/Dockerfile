FROM python:3.11-slim

WORKDIR /app

COPY app.py .
COPY index.html .

RUN pip install fastapi uvicorn psycopg2-binary

EXPOSE 8080

CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8081"]

