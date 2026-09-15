#!/usr/bin/env bash
# postStartCommand for the Deephaven Core devcontainer: build the Deephaven Python wheel and
# install it into the venv post-create.sh created, so `./gradlew server-jetty-app:run` (Python
# flavor) works with no manual step. The same sequence as the build-and-install command in
# AGENTS.md, minus the uninstall — this only ever runs when nothing is installed.
#
# postStart rather than postCreate: `py-server:assemble` builds the wheel inside a container,
# so it needs the podman socket at $DOCKER_HOST, which the podman-as-docker Feature only starts
# in its own postStartCommand. Feature hooks run before the workspace's in the same phase, so
# this is the earliest point it can work.
#
# Synchronous, not backgrounded: the guard below is not atomic, so an overlapping manual run
# could put two installs into one venv. A slow or failing build is handled by exiting 0 and
# logging instead.
#
# Steady-state cost is one `pip show`; the real work happens once per rebuild, ~10-15s.
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

# Best-effort throughout: a failing postStartCommand is not worth a container that will not
# come up, and every failure here is fixable from inside a running one.
finish() {
  exit 0
}
trap finish EXIT

log "=== post-start begin (pwd=$PWD) ==="

# --- serialize against any concurrent run ----------------------------------------------
#
# The guard and the install are not atomic together, so two runs — the hook and a manual one,
# most likely — could both pass the guard and race inside the same venv. -w rather than a bare
# block, so a stuck holder cannot hang container start indefinitely.
exec 9> "$LOCK" 2> /dev/null || true
if ! flock -w 1800 9 2> /dev/null; then
  say "another instance is still running (lock held) — skipping"
  exit 0
fi

# --- already installed? -----------------------------------------------------------------
#
# Install-once: a start hook cannot cheaply tell whether py/server changed. That also blocks a
# deliberate rerun after editing py/server, which is what the AGENTS.md build-and-install
# command is for.
if pip show deephaven-core > /dev/null 2>&1; then
  log "deephaven-core already installed — nothing to do"
  log "hint: edited py/server? this installs once — see build-and-install in AGENTS.md."
  exit 0
fi

# --- is the podman API socket actually up? ----------------------------------------------
#
# Checked up front so the common ordering failure names itself, rather than surfacing as an
# opaque gradle/docker-java connection error minutes into a build.
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
# By *path*, not by name with `--find-links`: that only adds a search location, so pip merges
# the local wheel with PyPI and takes the highest version — not necessarily the one just built.
# --no-index is not the alternative, since the [autocomplete] extra resolves from the index;
# naming the file pins just the deephaven-core distribution. Newest by mtime, though gradle
# syncs this directory, so it should hold one wheel.
WHEEL="$(ls -1t "$WHEEL_DIR"/deephaven_core-*.whl 2> /dev/null | head -n1)"
if [ -z "$WHEEL" ]; then
  say "no wheel found in $WHEEL_DIR after a successful build — skipping pip (see $LOG)"
  exit 0
fi
log "installing $WHEEL"

t1=$SECONDS
if pip install "./${WHEEL}[autocomplete]" >> "$LOG" 2>&1; then
  say "installed $(pip show deephaven-core 2> /dev/null | awk '/^Version:/{print $2}') into $VIRTUAL_ENV ($((SECONDS - t1))s)"
else
  say "pip install FAILED after $((SECONDS - t1))s — see $LOG"
fi

log "=== post-start end ==="
