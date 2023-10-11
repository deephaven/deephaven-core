#!/bin/bash

set -euo pipefail

if [ "$#" -ne 2 ]; then
    echo "Usage: $0 out.xml out.log" 1>&2
    exit 1
fi

if [ -z "${DH_PREFIX}" ]; then
    echo "$0: Environment variable DH_PREFIX is not set, aborting." 1>&2
    exit 1
fi

cd $DH_PREFIX/src/rdeephaven

OUT_XML="$1"
OUT_LOG="$2"

R --no-save <<"EOF" >& "${OUT_LOG}"
library('testthat')
options(testthat.output_file = '${OUT_XML}')
test_package('rdeephaven', reporter = 'junit')
EOF

if [ ! -f "${OUT_XML}" ]; then
    echo "$0: No test result was produced, failed." 1>&2
    exit 1
fi

NFAILURES=$(grep -o 'failures="[0-9]*"' | grep -v 'failures="0"' | wc -l)
NERRORS=$(grep -o 'errors="[0-9]*' | grep -v 'errors="0"' | wc -l)

if [ "${NFAILURES}" -ne 0 ] || [ "${NERRORS}" -ne 0 ]; then
    echo "$0: Test run had ${NFAILURES} failures and ${NERRORS} errors, failed." 1>&2
    exit 1
fi

exit 0
