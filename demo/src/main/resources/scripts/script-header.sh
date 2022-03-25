#!/usr/bin/env bash

#set -o nounset
set -o errexit
set -o xtrace

# Save some logs for us to debug any issues
LOG_FILE="/var/log/vm-startup.log-$(date "+%Y%m%d%H%M%S")"
sudo ln -nsf $LOG_FILE /var/log/vm-startup.log
exec > >(tee "$LOG_FILE" | logger -t vm-startup -s 2>/dev/console) 2>&1

# Prepare a nice logging function
SCRIPT_NAME=$(basename "${BASH_SOURCE[0]}")
H_NAME="$(hostname)"
function log() {
  echo "$SCRIPT_NAME -- $H_NAME -- $(date) $*"
}

# TODO: validate certificates are not expired

[[ "active" == $(systemctl is-active dh) ]] && {
    log "dh service is active, waiting for server to startup"
    # we'll wait up to 100 seconds before giving up and re-initializing system
    tries=100
    while ! (( tries-- )) && ! curl -k https://localhost:10000 &> /dev/null; do
        echo -n '.'
        (( tries % 10 )) || echo
        sleep 1
    done
    (( tries )) && {
        log "Server is running on port 10000, exiting early"
        exit 0
    }
    log "Server did not come up on port 10000 after 100 tries"
}

MY_UNAME="${MY_UNAME:-$(id -un)}"
MY_GNAME="${MY_GNAME:-$(id -gn)}"

# Prepare the deephaven working directories
export DH_DIR="${DH_DIR:-/dh}"
export DATA_DIR="${DATA_DIR:-$DH_DIR/data}"
export DH_SSL_DIR="${DH_SSL_DIR:-/etc/ssl/dh}"
# instead of /tmp/scratch, lets force new certs to be generated in /etc/ssl/internal
export AUTH_DIR=/etc/ssl/internal
test -d "$DH_DIR" ||
    sudo mkdir -p "$DH_DIR"
test "${MY_UNAME}:${MY_GNAME}" == "$(stat -c %U:%G "$DH_DIR")" ||
    sudo chown "$(id -un):$(id -gn)" "$DH_DIR"
test -d "${AUTH_DIR}" ||
    sudo mkdir -p "${AUTH_DIR}"
test -d "$DATA_DIR" ||
    mkdir -p "$DATA_DIR"
test -d "$DH_SSL_DIR" ||
    sudo mkdir -p "$DH_SSL_DIR"