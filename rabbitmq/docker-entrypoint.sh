#!/bin/bash
set -e

# Активируем Prometheus plugin автоматически
rabbitmq-plugins enable rabbitmq_prometheus

# Запускаем RabbitMQ
exec docker-entrypoint.sh "$@"
