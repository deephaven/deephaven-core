#!/usr/bin/env bash

set -o errexit
set -o pipefail
set -o nounset

# CI only, for the Nightly Python Versions CI workflow. Makes a fresh venv for one Python version, installs the
# Deephaven wheels and the test dependencies, then calls py/run-tests-venv.sh. Developers call that script in their own
# venv, see py/<suite>/TESTING.md.
# Each run gets the newest patch release of the Python version that uv knows and the newest release of every
# dependency, so the matrix catches breaking releases early.
#
# usage: py-versions-test.sh <server|client> <python-version> [test ...]
#   python-version  any version uv accepts, for example 3.10, 3.14 or 3.14t (free-threaded)
#   test            optional unittest names, passed on to run-tests-venv.sh. Default: all of tests/.
#
# Needs uv and the three wheels: ./gradlew :py-embedded-server:assemble :py-server:assemble :py-client:assemble

if [[ $# -lt 2 || ( "$1" != server && "$1" != client ) ]]; then
    echo "usage: $0 <server|client> <python-version> [test ...]" >&2
    exit 2
fi
SUITE=$1
PY_VERSION=$2
shift 2

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
VENV="${ROOT}/py/${SUITE}/build/py-versions/${PY_VERSION}/venv"

# deephaven-server pins deephaven-core to its own version, so all three wheels must come from one build.
WHEELS=()
shopt -s nullglob
# Quoted, so nullglob doesn't expand them against the current folder, where they match nothing.
for pattern in 'embedded-server/build/wheel/deephaven_server-*.whl' 'server/build/wheel/deephaven_core-*.whl' \
        'client/build/wheel/pydeephaven-*.whl'; do
    found=("${ROOT}"/py/${pattern})
    if [[ ${#found[@]} -ne 1 ]]; then
        echo "Expected one py/${pattern}, found ${#found[@]}." >&2
        echo "Run: ./gradlew :py-embedded-server:assemble :py-server:assemble :py-client:assemble" >&2
        exit 1
    fi
    WHEELS+=("${found[0]}")
done
shopt -u nullglob

rm -rf "${VENV}"
# --managed-python uses uv's own Python build, at the newest patch release. Without it, uv takes any matching installed
# Python, which can be many patches old.
uv venv --quiet --managed-python --python "${PY_VERSION}" "${VENV}"
uv pip install --quiet --python "${VENV}/bin/python" "${WHEELS[@]}"
# One at a time, so a package with no wheel for this Python doesn't block the rest. Tests that need it skip or fail.
while IFS= read -r req; do
    req="${req%%#*}"
    [[ -z "${req// /}" ]] && continue
    uv pip install --quiet --python "${VENV}/bin/python" "${req}" || echo "Skipping ${req}: not installable on ${PY_VERSION}"
done < "${ROOT}/py/${SUITE}/requirements-test.txt"

export VIRTUAL_ENV="${VENV}"
export PATH="${VENV}/bin:${PATH}"
exec "${ROOT}/py/run-tests-venv.sh" "${SUITE}" "$@"
