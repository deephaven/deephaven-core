#!/usr/bin/env bash

set -o errexit
set -o pipefail
set -o nounset

# We know the gradle daemon takes 1GiB (see gradle.properties)
DAEMON_BYTES=1073741824

# We'll leave some buffer space for other system resources / processes
OTHER_BYTES=2147483648

TOTAL_SYSTEM_BYTES="$(free --bytes | grep Mem | awk -F " " '{print $2}')"

# This is accounting for "worst case", assuming every single worker is using the theoretical maximum.
# Currently, engine/table/build.gradle sets a heap size of 6GiB, so that's the maximum.
PER_WORKER_BYTES=6442450944

MAX_WORKERS="$(( (TOTAL_SYSTEM_BYTES - DAEMON_BYTES - OTHER_BYTES) / PER_WORKER_BYTES ))"
MAX_WORKERS="$(( MAX_WORKERS > 0 ? MAX_WORKERS : 1 ))"

echo "org.gradle.workers.max=${MAX_WORKERS}"
