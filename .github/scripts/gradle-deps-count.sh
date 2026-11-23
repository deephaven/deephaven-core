#!/usr/bin/env bash

set -o errexit
set -o pipefail
set -o nounset

# Prints the number of artifact files in the Gradle dependency cache.
DIR="${GRADLE_USER_HOME:-${HOME}/.gradle}/caches/modules-2/files-2.1"
if [ -d "${DIR}" ]; then
  find "${DIR}" -type f | wc -l | tr -d ' '
else
  echo 0
fi
