#!/usr/bin/env bash
# postStartCommand for the Deephaven Core devcontainer — builds the Deephaven Python
# wheels and installs them into the venv post-create.sh created, so that the documented
# `./gradlew server-jetty-app:run` (the Python flavor) works without a manual setup step.
#
# Equivalent to running, by hand:
#
#   ./gradlew py-server:assemble
#   pip install --find-links py/server/build/wheel "deephaven-core[autocomplete]"
#
# postStart rather than postCreate: `py-server:assemble` builds the wheel *inside a
# container* (buildSrc/.../io.deephaven.python-wheel.gradle registers it as a docker task),
# so it needs the podman API socket at $DOCKER_HOST — and that socket is started by the
# podman-as-docker Feature's own postStartCommand. Feature lifecycle hooks run before the
# workspace's in the same phase, so postStart is the earliest point this can work at all;
# at postCreate it fails on connect.
#
# Run synchronously, NOT backgrounded. Backgrounding invites races: the `pip show` guard
# below is not atomic, so a hook run overlapping a manual run would put two `pip install`s
# into one venv and corrupt it, and a developer starting work mid-flight would see a
# half-installed `deephaven` package. The reason to background — keeping a `ruff`/`mypy`
# failure inside the wheel container from wedging container start — is handled instead by
# always exiting 0 and writing failures to the log, which costs nothing and races with
# nothing.
#
# Steady-state cost is one `pip show` (a fraction of a second); the real work happens once
# per rebuild, roughly 10-15s given the ~/.gradle volume declared in devcontainer.json and
# podman's own persistent image store.
set -u

LOG_DIR="$HOME/.cache/deephaven"
LOG="$LOG_DIR/devcontainer-post-start.log"
LOCK="/tmp/deephaven-devcontainer-post-start.lock"
WHEEL_DIR="py/server/build/wheel"

mkdir -p "$LOG_DIR" 2> /dev/null || true

log() {
  echo "$(date -Is) $*" >> "$LOG" 2> /dev/null || true
}

# Says it on the terminal (visible in the VS Code postStart output) *and* in the log.
say() {
  echo "deephaven-core: $*"
  log "$*"
}

# Everything below is best-effort. A failing postStartCommand is not worth a container that
# will not come up, and every failure here is recoverable from inside a running one — by
# rerunning this exact script by hand, which the flock makes safe to do at any time.
finish() {
  exit 0
}
trap finish EXIT

log "=== post-start begin (pwd=$PWD) ==="

# --- serialize against any concurrent run ----------------------------------------------
#
# The guard and the install are not atomic together, so two overlapping runs — the hook and
# a developer running this script directly, most likely — could both pass the guard and
# then race inside the same venv. flock makes the whole body mutually exclusive. -w rather
# than a bare block so a wedged holder cannot hang container start indefinitely.
exec 9> "$LOCK" 2> /dev/null || true
if ! flock -w 1800 9 2> /dev/null; then
  say "another instance is still running (lock held) — skipping"
  exit 0
fi

# --- already installed? -----------------------------------------------------------------
#
# Deliberately install-once. Rebuilding the wheel whenever py/server changes is not
# something a start hook can detect cheaply or correctly, so after editing py/server rerun
# this script by hand (it is safe to run at any time) or reinstall the wheel yourself.
if pip show deephaven-core > /dev/null 2>&1; then
  log "deephaven-core already installed — nothing to do"
  exit 0
fi

# --- is the podman API socket actually up? ----------------------------------------------
#
# Checked explicitly so the common ordering failure reports itself, rather than surfacing as
# an opaque gradle/docker-java connection error several minutes into a build.
if [ -n "${DOCKER_HOST:-}" ]; then
  SOCK_PATH="${DOCKER_HOST#unix://}"
  if [ ! -S "$SOCK_PATH" ]; then
    say "podman API socket $SOCK_PATH is not up — skipping wheel install (see $LOG)"
    log "DOCKER_HOST=$DOCKER_HOST but no socket at $SOCK_PATH"
    exit 0
  fi
fi

# --- build ------------------------------------------------------------------------------
say "building Deephaven Python wheels (first start after a rebuild; see $LOG)"
t0=$SECONDS
if ! ./gradlew py-server:assemble >> "$LOG" 2>&1; then
  say "py-server:assemble FAILED after $((SECONDS - t0))s — venv left empty, see $LOG"
  log "hint: the wheel container runs mypy and 'ruff check'/'ruff format --check' over"
  log "hint: py/server, so a formatting or typing drift there fails this build."
  exit 0
fi
log "py-server:assemble ok after $((SECONDS - t0))s"

# --- install ----------------------------------------------------------------------------
#
# Gated on the build above having succeeded AND on a wheel actually being present.
# `--find-links` only *adds* a search location; it does not disable the index, so a run
# where the build silently produced nothing would happily install the latest release from
# PyPI instead — a version-mismatched deephaven-core against a snapshot server, reported as
# success. --no-index is not the fix here because the [autocomplete] extra legitimately
# resolves from the index; checking for the artifact is.
if ! ls "$WHEEL_DIR"/deephaven_core-*.whl > /dev/null 2>&1; then
  say "no wheel found in $WHEEL_DIR after a successful build — skipping pip (see $LOG)"
  exit 0
fi

t1=$SECONDS
if pip install --find-links "$WHEEL_DIR" "deephaven-core[autocomplete]" >> "$LOG" 2>&1; then
  say "installed $(pip show deephaven-core 2> /dev/null | awk '/^Version:/{print $2}') into $VIRTUAL_ENV ($((SECONDS - t1))s)"
else
  say "pip install FAILED after $((SECONDS - t1))s — see $LOG"
fi

log "=== post-start end ==="
