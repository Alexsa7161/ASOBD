#!/bin/bash


sleep 30

docker exec clickstream-rabbitmq2 rabbitmqctl stop_app
docker exec clickstream-rabbitmq2 rabbitmqctl join_cluster rabbit@rabbitmq1
docker exec clickstream-rabbitmq2 rabbitmqctl start_app

docker exec clickstream-rabbitmq3 rabbitmqctl stop_app
docker exec clickstream-rabbitmq3 rabbitmqctl join_cluster rabbit@rabbitmq1  
docker exec clickstream-rabbitmq3 rabbitmqctl start_app

docker exec clickstream-rabbitmq1 rabbitmqctl set_policy HA ".*" '{"ha-mode":"all","ha-sync-mode":"automatic"}'
