#!/bin/sh
# postCreateCommand for the Deephaven Core devcontainer: create the Python venv, and repair
# ownership of the named-volume mount points.
#
# Runs at container-create time (~15s, repeated on every rebuild) as the remote user, so
# anything touching /usr/local needs sudo — `-n` everywhere, so a passworded sudo fails fast
# instead of hanging on a prompt nobody can answer.
#
# Every path exits 0: a failing postCreateCommand aborts container creation, and every failure
# here is both loud and fixable from inside a running container.
set -u

warn() {
  echo "deephaven-core: $*" >&2
}

# --- 1. Python virtualenv ----------------------------------------------------------------
#
# Creates the venv devcontainer.json's remoteEnv points at (VIRTUAL_ENV, bin/ on PATH).
#
# Outside the workspace on purpose: that is a bind mount (virtiofs/gRPC-FUSE on macOS and
# Windows) and a venv is thousands of small files, so imports and `pip install` are measurably
# slower there. It also keeps it away from `git clean -xfd`, gradle's file watcher and spotless.
#
# A venv rather than a global install — which this python would allow — so the python state is
# disposable on its own: `rm -rf $VIRTUAL_ENV && python3 -m venv $VIRTUAL_ENV`, seconds, never a
# container rebuild. With bin/ on PATH it behaves like a global install anyway.
#
# Created empty. The Deephaven wheel is installed by post-start.sh instead, because building it
# needs the podman socket that the podman-as-docker Feature only starts at postStart.
VENV_DIR="/usr/local/share/deephaven-core/venv"
VENV_PARENT="$(dirname "$VENV_DIR")"

if [ ! -x "$VENV_DIR/bin/python" ]; then
  # /usr/local/share is root-owned; hand the parent over before creating the venv unprivileged.
  if sudo -n mkdir -p "$VENV_PARENT" 2> /dev/null &&
    sudo -n chown "$(id -un)" "$VENV_PARENT" 2> /dev/null; then
    python3 -m venv "$VENV_DIR" || warn "python3 -m venv failed — \$VIRTUAL_ENV is empty"
  else
    warn "could not create $VENV_PARENT (no passwordless sudo?) — venv not created"
  fi
fi

# --- 2. Ownership of the named-volume mount points ----------------------------------------
#
# Docker seeds a new named volume from whatever the image has at the target, carrying that
# path's ownership with it. None of these targets exist in the image, so the mount point is
# created root-owned inside an otherwise vscode-owned tree and the first write fails:
#
#   ~/.gradle        ./gradlew dies with "Could not create parent directory for lock file",
#                    before it starts a build, so it looks nothing like a gradle problem
#   */node_modules   npm ci fails with EACCES
#
# One-level chown, not -R: anything written underneath already belongs to whoever wrote it.
# Guarded on the directory being unwritable, so it is a no-op once a volume has been fixed.
# The node-nvmrc Feature repairs only the one directory it is configured with, so keep this
# list in sync with devcontainer.json. Relative paths resolve against the workspace folder,
# where postCreateCommand runs.
for d in \
  "$HOME/.gradle" \
  proto/raw-js-openapi/node_modules \
  web/client-api/types/node_modules; do
  if [ -d "$d" ] && [ ! -w "$d" ]; then
    sudo -n chown "$(id -un):$(id -gn)" "$d" 2> /dev/null ||
      warn "could not chown $d — writes there will fail"
  fi
done

exit 0
