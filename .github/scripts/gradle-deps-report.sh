#!/usr/bin/env bash

set -o errexit
set -o pipefail
set -o nounset

# Reports how many dependency files this job downloaded since the gradle-setup action restored the cache. Warns when
# this week's exact entry was restored and files were still downloaded, which means the seed missed them.
BEFORE="${GRADLE_DEPS_FILES_BEFORE:-0}"
AFTER="$(.github/scripts/gradle-deps-count.sh)"
DOWNLOADED=$(( AFTER - BEFORE ))
MESSAGE="Gradle dependency files: ${BEFORE} restored, ${DOWNLOADED} downloaded"
echo "${MESSAGE}"
if [ -n "${GITHUB_STEP_SUMMARY:-}" ]; then
  echo "${MESSAGE}" >> "${GITHUB_STEP_SUMMARY}"
fi
if [ "${GRADLE_DEPS_CACHE_HIT:-}" == "true" ] && [ "${DOWNLOADED}" -gt 0 ]; then
  echo "::warning title=Gradle dependency cache::${DOWNLOADED} dependency files were downloaded despite an exact cache hit; the seed in gradle-deps-cache.yml missed them"
fi
