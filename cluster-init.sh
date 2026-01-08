#!/bin/sh
set -e

echo "🔥 Starting RabbitMQ cluster initialization..."

# Ожидание готовности всех нод (30 сек)
echo "⏳ Waiting for all RabbitMQ nodes..."
sleep 10

echo "1️⃣ Stop apps on nodes 2,3"
docker exec clickstream-rabbitmq2 rabbitmqctl stop_app || true
docker exec clickstream-rabbitmq3 rabbitmqctl stop_app || true

echo "2️⃣ Join cluster rabbitmq2 → rabbitmq1"
docker exec clickstream-rabbitmq2 rabbitmqctl reset || true
docker exec clickstream-rabbitmq2 rabbitmqctl join_cluster rabbit@rabbitmq1

echo "3️⃣ Join cluster rabbitmq3 → rabbitmq1"
docker exec clickstream-rabbitmq3 rabbitmqctl reset || true
docker exec clickstream-rabbitmq3 rabbitmqctl join_cluster rabbit@rabbitmq1

echo "4️⃣ Start apps"
docker exec clickstream-rabbitmq2 rabbitmqctl start_app
docker exec clickstream-rabbitmq3 rabbitmqctl start_app

echo "5️⃣ HA Policy"
docker exec clickstream-rabbitmq1 rabbitmqctl set_policy HA "^" '{"ha-mode":"all","ha-sync-mode":"automatic"}' --apply-to queues

echo "✅ CLUSTER STATUS:"
docker exec clickstream-rabbitmq1 rabbitmqctl cluster_status | grep running_nodes

echo "🎉 CLUSTER READY! http://localhost:15672"
