#!/bin/bash

set -eu

if [ $(docker ps -q | wc -l) -gt 0 ]; then
    echo "$0: some docker containers seem to be running, aborting." 1>&2
    exit 1
fi

if [ $# -ne 2 -o \( "$1" != 'dh' -a "$1" != 'mz' \) ]; then
    echo "Usage: $0 dh|mz per_second_rate" 1>&2
    exit 1
fi

engine=$1
rate=$2

PERF_TAG=$(date -u '+%Y.%m.%d.%H.%M.%S_%Z')_${engine}_${rate}
export PERF_TAG

LOGS_DIR=./logs
if [ ! -d "$LOGS_DIR" ]; then
    mkdir $LOGS_DIR
fi

OUT_DIR="${LOGS_DIR}/${PERF_TAG}"
mkdir -p "$OUT_DIR"

LOG="$OUT_DIR/start.log"
rm -f $LOG

if [ "$engine" = "mz" ]; then
    docker compose up -d mysql redpanda debezium loadgen materialized mzcli >> $LOG 2>&1
elif [ "$engine" = "dh" ]; then
    docker compose up -d mysql redpanda debezium loadgen server grpc-proxy envoy web >> $LOG 2>&1
else
    echo "$0: Internal error, aborting." 1>&2
    exit 1
fi

# fire and forget; will stop when compose stops.
COMPOSE_LOG="${OUT_DIR}/docker-compose.log"
(nohup docker compose logs -f >& $COMPOSE_LOG < /dev/null &)

# avoid race with creation of log file above
sleep 0.2

# Wait till loadgen simulation started
echo -n "Waiting for loadgen simulation to start... " >> $LOG 2>&1
tail -f "$COMPOSE_LOG" | sed -n '/loadgen.*Simulated [0-9]* pageview actions in the last/ q'
echo "done." >> $LOG 2>&1

echo "${PERF_TAG}"

exit 0
