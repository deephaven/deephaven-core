#!/bin/bash

set -ex

exec docker-compose stop mysql redpanda debezium loadgen
