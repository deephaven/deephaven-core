#!/bin/bash
set -o errexit
set -o pipefail
set -o nounset
set -o xtrace

XML=${1};

# Start a tcpdump in the background
tcpdump -C 10 -Z root -i any -U -w /out/tcpdump.pcap &

# Pause for a moment to let the dump start
sleep 1

# Run the tests
go test -vet=all -v ./... 2>&1 | go-junit-report -set-exit-code -iocopy -out "$XML" && ret=$? || ret=$?;

# Pause another moment to ensure all packets were captures
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
