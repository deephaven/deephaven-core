#!/bin/bash

set -eu

usage_and_exit() {
    echo "Usage: $0 dh|mz per_second_rate wait_seconds top_samples top_delay_seconds" 1>&2
    exit 1
}

if [ "$#" -ne 5 ]; then
    usage_and_exit
fi

if [ "$1" != 'dh' -a "$1" != 'mz' ]; then
    usage_and_exit
fi

engine="$1"
rate_per_s="$2"
wait_s="$3"
top_samples="$4"
top_delay="$5"

echo "About to run an experiment for ${engine} with ${rate_per_s} pageviews/s."
echo
echo "Actions that will be performed in this run:"
echo "1. Start compose services required for for ${engine} and initialize simulation."
echo "2. Execute demo in ${engine} and setup update delay logging."
echo "3. Set ${rate_per_s} pageviews per second rate."
echo "4. Wait ${wait_s} seconds."
echo "5. Take ${top_samples} samples for mem and CPU utilization, ${top_delay} seconds between samples."
echo "6. Stop and 'reset' (down) compose."
echo
echo "Running experiment."
echo
echo "1. Starting compose and initializing simulation."
export PERF_TAG=$(./start_perf_run.sh "$engine" "$rate_per_s")
echo "PERF_TAG=${PERF_TAG}"
echo
echo "Logs are being saved to logs/$PERF_TAG."
echo

echo "2. Running demo in ${engine} and sampling delays."
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
echo

echo "3. Setting pageviews per second"
./set_pageviews_per_second.sh $rate_per_s
echo

echo "4. Waiting for $wait_s seconds."
sleep "$wait_s"
echo

echo "5. Sampling top for ${top_samples} * ${top_delay} seconds."
./sample_top.sh "$engine" "$top_samples" "$top_delay"
echo

echo "6. Stopping and 'reset' (down) compose."

./stop_all.sh
echo
echo "Experiment finished."

exit 0
