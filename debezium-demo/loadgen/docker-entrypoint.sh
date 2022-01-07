#!/bin/bash

set -e

wait-for-it --timeout=60 mysql:3306
wait-for-it --timeout=60 debezium:8083

cd /loadgen

exec python generate_load.py
