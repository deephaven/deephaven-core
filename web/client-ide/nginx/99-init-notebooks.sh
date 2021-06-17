#!/usr/bin/env bash
set -o errexit
set -o pipefail
set -o nounset

mkdir -p /data/notebooks
chown nginx /data/notebooks