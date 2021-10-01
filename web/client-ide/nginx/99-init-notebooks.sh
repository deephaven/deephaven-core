#!/usr/bin/env bash
set -o errexit
set -o pipefail
set -o nounset



test -d /data/notebooks || mkdir -p /data/notebooks
test "nginx" == "$(stat -c %U /data/notebooks)" || chown nginx /data/notebooks

test -d /data/layouts || mkdir -p /data/layouts
test "nginx" == "$(stat -c %U /data/layouts)" || chown nginx /data/layouts