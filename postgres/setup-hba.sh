#!/bin/bash
# Файл: postgres/setup-hba.sh
set -e

# Подменяем pg_hba.conf в volume на "trust" для всех подключений
cat > /var/lib/postgresql/data/pg_hba.conf <<EOF
# LOCAL
local   all             all                                     trust
# TCP ALL
host    all             all             0.0.0.0/0               trust
# REPLICATION
host    replication     replicator      0.0.0.0/0               trust
EOF

chown postgres:postgres /var/lib/postgresql/data/pg_hba.conf
