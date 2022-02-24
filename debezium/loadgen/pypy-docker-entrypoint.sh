#!/bin/bash --login

conda activate pypy

set -euo pipefail

wait-for-it --timeout 60 --service mysql:3306
wait-for-it --timeout 60 --service debezium:8083

cd /loadgen

exec pypy generate_load.py


