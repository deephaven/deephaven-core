#!/bin/sh
# postCreateCommand for the Deephaven Core devcontainer — the two setup steps that used to
# live in the local deephaven-core Feature's install.sh, moved here when that Feature was
# folded into devcontainer.json.
#
# Two consequences of the move, both deliberate:
#
#   - This runs at container-*create* time, not image-build time, so it is not a cached
#     layer and repeats on every rebuild. Measured at roughly 15s total, which is not worth
#     reintroducing a Dockerfile to avoid.
#   - It runs as the remote user, not root, so anything touching /usr/local or apt needs
#     sudo. The devcontainer provides passwordless sudo; -n everywhere so a passworded sudo
#     fails fast rather than hanging on a prompt nobody can answer.
#
# Every path exits 0. A failing postCreateCommand aborts container creation, and neither of
# these steps is worth an unbootable container: without the venv you get a loud Python
# error on first use, without the cache-volume chown gradle just cannot write its cache. Both
# are recoverable from inside a running container; a container that will not start is not.
set -u

warn() {
  echo "deephaven-core: $*" >&2
}

# --- 1. Python virtualenv --------------------------------------------------------------
#
# Creates the venv that devcontainer.json's remoteEnv points at (VIRTUAL_ENV, and its
# bin/ prepended to PATH).
#
# It lives outside the workspace:
#
#   - The workspace is a bind mount. On macOS and Windows that is virtiofs/gRPC-FUSE, and
#     a venv is thousands of small files — imports and `pip install` are measurably slower
#     there, and it is the one directory here with no reason to be host-visible.
#   - Nothing on the host to .gitignore, and nothing for `git clean -xfd`, gradle's file
#     watcher, or spotless to walk.
#   - One venv per container rather than one per git worktree. The container is already
#     the isolation boundary.
#
# Kept as a venv rather than installing into the interpreter directly — even though this
# python is source-built, group-writable by the remote user and not PEP 668
# externally-managed, so global installs would work — because the python state should be
# disposable independently of the image. The wheel/jpy install loop (`py-server:assemble`
# then `pip install --find-links py/server/build/wheel "deephaven-core[autocomplete]"`)
# does get wedged, and recovery should be `rm -rf $VIRTUAL_ENV && python3 -m venv
# $VIRTUAL_ENV`, not a rebuild. With bin/ on the container PATH it behaves like a global
# install from every tool's point of view, so the usual cost of a venv — having to
# activate it — is not paid.
#
# Not --system-site-packages: the python Feature's linters and formatters are pipx installs
# in /usr/local/py-utils/bin, which stays on PATH on its own, so isolation costs nothing.
#
# Note the venv is created empty. The Deephaven wheels are NOT installed here: building
# them runs `py-server:assemble`, which builds the wheel inside a container and so needs
# the podman API socket — and that socket is only started by the podman-as-docker
# Feature's postStartCommand, which has not run yet at postCreate time. post-start.sh does
# that install, one lifecycle phase later; see its header.
VENV_DIR="/usr/local/share/deephaven-core/venv"
VENV_PARENT="$(dirname "$VENV_DIR")"

if [ ! -x "$VENV_DIR/bin/python" ]; then
  # /usr/local/share is root-owned, so the directory has to be made and handed over before
  # the unprivileged venv creation can write into it.
  if sudo -n mkdir -p "$VENV_PARENT" 2> /dev/null &&
    sudo -n chown "$(id -un)" "$VENV_PARENT" 2> /dev/null; then
    python3 -m venv "$VENV_DIR" || warn "python3 -m venv failed — \$VIRTUAL_ENV is empty"
  else
    warn "could not create $VENV_PARENT (no passwordless sudo?) — venv not created"
  fi
fi

# --- 2. Ownership of the gradle cache volume ---------------------------------------------
#
# The gradle-cache-${devcontainerId} mount in devcontainer.json targets ~/.gradle, a path
# that does not exist in the image. Docker seeds a new named volume from whatever the image
# has at the target and carries that path's ownership across with it — with nothing there
# to copy, it creates the mount point root-owned instead, sitting inside an otherwise
# vscode-owned home. The remote user then cannot write to its own gradle home and the first
# thing ./gradlew does is die:
#
#   Could not create parent directory for lock file
#   /home/vscode/.gradle/wrapper/dists/gradle-<v>-all/<hash>/gradle-<v>-all.zip.lck
#
# taking post-start.sh's wheel build down with it (it fails "after 0s" — the wrapper never
# gets as far as starting a build, so the failure looks nothing like a gradle problem).
#
# Only the mount point itself is wrong, so this is a one-level chown, not -R: anything
# gradle writes underneath is already owned by the user that wrote it. Guarded on the
# directory being unwritable, so it is a no-op on every rebuild that reuses a volume this
# has already fixed.
GRADLE_HOME="$HOME/.gradle"
if [ -d "$GRADLE_HOME" ] && [ ! -w "$GRADLE_HOME" ]; then
  sudo -n chown "$(id -un):$(id -gn)" "$GRADLE_HOME" 2> /dev/null ||
    warn "could not chown $GRADLE_HOME — ./gradlew cannot write its cache"
fi

exit 0
