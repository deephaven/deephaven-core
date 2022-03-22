#!/bin/sh

set -eu

if [ $# -ne 5 -o \( "$1" != 'dh' -a "$1" != 'mz' \) ]; then
    echo "Usage: $0 dh|mz per_second_rate wait_seconds top_samples top_delay_seconds" 1>&2
    exit 1
fi

engine="$1"
rate_per_s="$2"
wait_s="$3"
top_samples="$4"
top_delay="$5"

echo "Starting compose and waiting for simulation to start."
export PERF_TAG=$(./start_perf_run.sh "$engine" "$rate_per_s")
echo "Compose start done."
echo "PERF_TAG=${PERF_TAG}"

echo "Running demo in engine and sampling delays."
if [ "$engine" = "mz" ]; then
    ./mz_run_demo.sh
    ./mz_sample_dt.sh
elif [ "$engine" = "dh" ]; then
    ./dh_run_demo.sh
    ./dh_sample_dt.sh
else
    echo "$0: Internal error, aborting." 1>&2
    exit 1
fi

echo "Setting pageviews per second"
./set_pageviews_per_second.sh $rate_per_s

echo "Waiting for $wait_s seconds..."
sleep "$wait_s"

echo "Sampling top."
./sample_top.sh "$engine" "$top_samples" "$top_delay"

echo "Stopping compose."

./stop_all.sh

echo "Experiemnt finished."

exit 0
