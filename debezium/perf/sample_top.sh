#!/bin/bash

set -eu

usage_and_exit() {
    echo "Usage: $0 mz|dh rate nsamples delay_sec"
    exit 1
}

if [ "$#" -ne 3 ]; then
    usage_and_exit
fi

if [ "$1" != 'dh' -a "$1" != 'mz' ]; then
    usage_and_exit
fi

engine=$1
nsamples=$2
delay_s=$3

if [ "$engine" = "mz" ]; then
    proc_spec='materialize:^materialized redpanda:^/opt/redpanda/bin/redpanda'
elif [ "$engine" = "dh" ]; then
    proc_spec='deephaven:java.*deephaven redpanda:^/opt/redpanda/bin/redpanda'
else
    echo "$0: Internal error, aborting." 1>&2
    exit 1
fi

PROC_SPECS=$(python3 ./pid_from_cmdline.py $proc_spec)
exec python3 ./sample_top.py "$nsamples" "$delay_s" $PROC_SPECS
