#!/usr/bin/env bash

set -o errexit
set -o pipefail
set -o nounset

# Reports how many dependency files this job downloaded since the gradle-setup action restored the cache.
BEFORE="${GRADLE_DEPS_FILES_BEFORE:-0}"
AFTER="$(.github/scripts/gradle-deps-count.sh)"
MESSAGE="Gradle dependency files: ${BEFORE} restored, $(( AFTER - BEFORE )) downloaded"
echo "${MESSAGE}"
if [ -n "${GITHUB_STEP_SUMMARY:-}" ]; then
  echo "${MESSAGE}" >> "${GITHUB_STEP_SUMMARY}"
fi
