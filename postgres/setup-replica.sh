#!/bin/bash
set -e

echo "Waiting for postgres-master..."

# Ждём master под обычным пользователем
until pg_isready -h postgres-master -p 5432 -U clickstream; do
  echo "Master not ready, waiting..."
  sleep 5
done

echo "Cleaning data directory..."
rm -rf /var/lib/postgresql/data/* || true

echo "Starting basebackup..."
PGPASSWORD=clickstream pg_basebackup \
  -h postgres-master \
  -D /var/lib/postgresql/data \
  -U replicator \
  -P -v \
  --wal-method=stream \
  -R

chown -R postgres:postgres /var/lib/postgresql/data

echo "Replica ready - starting PostgreSQL"
exec docker-entrypoint.sh postgres
