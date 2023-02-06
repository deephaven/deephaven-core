#!/usr/bin/env bash
set -o errexit
set -o pipefail
set -o nounset

test -d /data/notebooks || sudo -n -u root /bin/mkdir -p /data/notebooks
test "nginx" == "$(stat -c %U /data/notebooks)" || sudo -n -u root /bin/chown nginx.nginx /data/notebooks

test -d /data/layouts || sudo -n -u root /bin/mkdir -p /data/layouts
test "nginx" == "$(stat -c %U /data/layouts)" || sudo -n -u root /bin/chown nginx.nginx /data/layouts