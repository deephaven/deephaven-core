#!/bin/bash

set -eu

if [ -z "$PERF_TAG" ]; then
    echo "$0: PERF_TAG environment variable is not defined, aborting." 1>&2
    exit 1
fi

DATA_TAG="mz_sample_dt"
OUT=logs/${PERF_TAG}/${DATA_TAG}.log

SCRIPT=$(cat <<'EOF'
while true; do
  DATE_TAG=$(date -u '+%Y-%m-%d %H:%M:%S%z')
  echo -n "$DATE_TAG|"
  psql --csv -A -t -f /scripts/sample_dt.sql -U materialize -h materialized -p 6875
  sleep 1
done
EOF
)

(nohup docker compose run -T --entrypoint /bin/bash mzcli -c "$SCRIPT" < /dev/null >& $OUT &)

exit 0
