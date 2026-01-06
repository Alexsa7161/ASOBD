#!/usr/bin/env bash
set -e

# Генерация FERNET_KEY, если не задана
if [ -z "$AIRFLOW_FERNET_KEY" ]; then
    export AIRFLOW_FERNET_KEY=$(python -c 'from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())')
fi

# Обновляем базу
airflow db upgrade

# Создаем пользователя admin, если его нет
airflow users create \
    --username admin \
    --password admin \
    --firstname Admin \
    --lastname Admin \
    --role Admin \
    --email admin@example.com || true

# Запускаем вебсервер и планировщик
exec airflow webserver & exec airflow scheduler
