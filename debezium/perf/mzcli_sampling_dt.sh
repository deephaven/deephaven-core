#!/bin/bash

set -ex

DATE_TAG=$(date '+%Y.%m.%d.%H.%M.%S_%Z')
DATA_TAG="mz_sample_dt"
OUT=logs/${DATE_TAG}_${DATA_TAG}.log

(nohup docker-compose run -T --entrypoint /bin/bash mzcli -c "
while true; do
  psql --csv -A -t -f /scripts/sample_dt.sql -U materialize -h materialized -p 6875
  sleep 1
done" < /dev/null >& $OUT &)

