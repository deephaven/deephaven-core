#!/bin/bash

set -ex

exec docker-compose up -d mysql redpanda debezium loadgen server grpc-proxy envoy web
