#!/usr/bin/env bash

set -o errexit
set -o pipefail
set -o nounset

if [[ $(git ls-files -i --exclude-standard | wc -c) -ne 0 ]]; then
  >&2 echo ".gitignore rules is inconsistent with the checked in files"
  >&2 git ls-files -i --exclude-standard
  exit 1
fi

exit 0