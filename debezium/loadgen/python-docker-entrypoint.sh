#!/bin/bash

set -e

wait-for-it --timeout 60 --service mysql:3306
wait-for-it --timeout 60 --service debezium:8083

cd /loadgen

exec python generate_load.py
