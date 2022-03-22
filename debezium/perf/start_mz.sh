#!/bin/bash

set -ex

LOGS_DIR=./logs
if [ ! -d "$LOGS_DIR" ]; then
    mkdir $LOGS_DIR
fi

exec docker-compose up -d mysql redpanda debezium loadgen materialized mzcli
