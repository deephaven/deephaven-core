#!/bin/bash
set -o errexit
set -o pipefail
set -o nounset
set -o xtrace

XML_DIR=${1};

# Start a tcpdump in the background
tcpdump -C 10 -Z root -i any -U -w /out/report/tcpdump.pcap &

# Pause for a moment to let the dump start
sleep 1

# Run the tests
python -m xmlrunner discover tests -v -o "$XML_DIR" && ret=$? || ret=$?;

# Pause another moment to ensure all packets were captures
sleep 1

# Stop tcpdump
kill %1;

# Check if the tests passed, emit correct exit code
if [ $ret -eq 0 ] ; then
    echo "Tests passed, deleting pcap"
    rm /out/report/tcpdump*
else
    echo "Tests failed, emitting exit code $ret"
    exit $ret
fi
