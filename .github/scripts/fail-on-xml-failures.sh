#!/usr/bin/env bash

set -o errexit
set -o pipefail
set -o nounset

# TODO (core#567)
# Would be great to replace this with an official action, since this is fragile.
if [[ $(find . -type f -path "*/build/test-results/*" -name "TEST-*.xml" -exec sed -n '2p' {} \; | grep -v "failures=\"0\"" | head --bytes 1 | wc --bytes) -ne 0 ]]; then
    echo "Found XML unit test failures, exiting with 1"
    exit 1
fi
