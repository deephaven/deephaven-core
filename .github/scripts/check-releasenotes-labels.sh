#!/usr/bin/env bash

set -o errexit
set -o pipefail
set -o nounset

if [ "${HAS_ReleaseNotesNeeded}" == "true" ] ; then
  if [ "${HAS_NoReleaseNotesNeeded}" == "true" ] ; then
    >&2 echo "Conflicting release notes requirements"
    exit 1
  fi
  exit 0
fi

if [ "${HAS_NoReleaseNotesNeeded}" == "true" ] ; then
  exit 0
fi

>&2 echo "No release notes requirements found"
exit 1
