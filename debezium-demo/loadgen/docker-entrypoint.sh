#!/bin/bash

set -e

wait-for-it --timeout 60 --service mysql:3306
wait-for-it --timeout 60 --service debezium:8083

cd /loadgen

# -u for unbuffered STDOUT so that we get docker-compose log visibility.
exec python -u generate_load.py
