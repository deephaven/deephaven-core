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
# Test heaps are at most 3500MiB (see engine/table/build.gradle); 4GiB covers that plus JVM overhead (metaspace, code
# cache, GC structures, thread stacks). On the standard 16GiB GitHub runners this yields 3 workers.
PER_WORKER_BYTES=4294967296

# See https://github.com/gradle/gradle/issues/14431#issuecomment-1601724453 for why we need to have this sort of logic
# here
MAX_WORKERS="$(( (TOTAL_SYSTEM_BYTES - DAEMON_BYTES - OTHER_BYTES) / PER_WORKER_BYTES ))"
MAX_WORKERS="$(( MAX_WORKERS > 0 ? MAX_WORKERS : 1 ))"

# Each argument is a Java version we expect to exist of the form JAVA_HOME_<version>_X64 (based on the output of the
# https://github.com/actions/setup-java)
JAVA_INSTALL_PATHS=""
for version in "$@"; do
    var_name="JAVA_HOME_${version}_X64"
    if [ -n "${!var_name:-}" ]; then
        JAVA_INSTALL_PATHS="${JAVA_INSTALL_PATHS}${!var_name},"
    else
      echo "Error: ${var_name} is not set" >&2
      exit 1
    fi
done

# Our CI JDKs should be pre-provisioned and invoked correctly,
# we shouldn't rely on gradle for any of this logic.
cat << EOF
org.gradle.java.installations.auto-download=false
org.gradle.java.installations.auto-detect=false
org.gradle.workers.max=${MAX_WORKERS}
org.gradle.java.installations.paths=${JAVA_INSTALL_PATHS}
EOF

# Ensure we remove the -SNAPSHOT qualifier for release branch workflows
if [[ "${GITHUB_REF}" == refs/heads/release/v* ]];
then
  echo "deephavenBaseQualifier="
fi
