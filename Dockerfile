FROM python:3.9

WORKDIR /app

# Копируем зависимости
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Копируем весь проект
COPY . .

# Запуск по умолчанию — твой скрипт
CMD ["python", "run_tests.py"]
