#!/usr/bin/env bash

__dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "Installing tensorflow stuff"

set -o errexit
set -o pipefail
set -o nounset
set -o xtrace

pip install -r "${__dir}/requirements.txt"