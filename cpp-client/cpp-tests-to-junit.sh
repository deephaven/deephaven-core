#!/bin/bash

set -o errexit
set -o pipefail
set -o nounset
set -o xtrace

if [ "$#" -ne 2 ]; then
    echo "Usage: $0 out.xml out.log" 1>&2
    exit 1
fi

if [ -z "${DH_PREFIX}" ]; then
    echo "$0: Environment variable DH_PREFIX is not set, aborting." 1>&2
    exit 1
fi
# Start a tcpdump in the background
tcpdump -C 10 -Z root -i any -U -w /out/tcpdump.pcap &

# Pause for a moment to let the dump start
sleep 1

${DH_PREFIX}/bin/dhclient_tests --reporter XML --out "$1" 2>&1 | tee "$2" && ret=$? || ret=$?

# Pause another moment to ensure all packets were captured
sleep 1

# Stop tcpdump
kill %1;

# Check if the tests passed, emit correct exit code
if [ $ret -eq 0 ] ; then
    echo "Tests passed, deleting pcap"
    rm /out/tcpdump*
else
    echo "Tests failed, emitting exit code $ret"
    exit $ret
fi
