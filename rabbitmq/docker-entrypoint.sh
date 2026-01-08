#!/bin/bash

set -e

rabbitmq-plugins enable rabbitmq_prometheus

exec docker-entrypoint.sh "$@"
