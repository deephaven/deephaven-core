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

${DH_PREFIX}/bin/dhclient_tests --reporter XML --out "$1" 2>&1 | tee "$2"
