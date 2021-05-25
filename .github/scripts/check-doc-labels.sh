#!/usr/bin/env bash

set -o errexit
set -o pipefail
set -o nounset

if [ "${HAS_DocumentationNeeded}" == "true" ] ; then
  if [ "${HAS_NoDocumentationNeeded}" == "true" ] ; then
    >&2 echo "Conflicting documentation requirements"
    exit 1
  fi
  exit 0
fi

if [ "${HAS_NoDocumentationNeeded}" == "true" ] ; then
  exit 0
fi

>&2 echo "No documentation requirements found"
exit 1
