#!/usr/bin/env bash

set -o errexit
set -o pipefail
set -o nounset

# Runs the Python server or client tests on one Python version, for the Nightly Python Versions CI workflow. It makes a
# fresh venv, installs the Deephaven packages and the test dependencies, then calls py/run-tests-venv.sh. Developers
# who want to run the tests in their own venv call that script directly, see py/<suite>/TESTING.md.
#
# usage: py-versions-test.sh <server|client> <python-version> [test ...]
#   python-version  any version uv accepts, for example 3.10, 3.14 or 3.14t (free-threaded)
#   test            optional unittest names, passed on to run-tests-venv.sh. Default: all of tests/.
#
# Needs uv and the deephaven-server wheel: ./gradlew :py-embedded-server:assemble

if [[ $# -lt 2 || ( "$1" != server && "$1" != client ) ]]; then
    echo "usage: $0 <server|client> <python-version> [test ...]" >&2
    exit 2
fi
SUITE=$1
PY_VERSION=$2
shift 2

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
VENV="${ROOT}/py/${SUITE}/build/py-versions/${PY_VERSION}/venv"

shopt -s nullglob
WHEELS=("${ROOT}"/py/embedded-server/build/wheel/deephaven_server-*.whl)
shopt -u nullglob
if [[ ${#WHEELS[@]} -ne 1 ]]; then
    echo "Expected one deephaven-server wheel in py/embedded-server/build/wheel, found ${#WHEELS[@]}." >&2
    exit 1
fi
# py/server/setup.py and py/client/setup.py read their version from DEEPHAVEN_VERSION. It must match the wheel's
# version, because deephaven-server pins deephaven-core to it.
DEEPHAVEN_VERSION="$(basename "${WHEELS[0]}" | cut -d- -f2)"
export DEEPHAVEN_VERSION

rm -rf "${VENV}"
# --managed-python makes uv use its own Python build, which it downloads at the newest patch release it knows. Without
# it, uv takes any matching Python already installed, which can be many patch releases old.
uv venv --quiet --managed-python --python "${PY_VERSION}" "${VENV}"
# --reinstall-package rebuilds the local packages. Without it, uv can reuse a cached build of older source, because the
# package version does not change between edits.
uv pip install --quiet --python "${VENV}/bin/python" --reinstall-package deephaven-core --reinstall-package pydeephaven \
    "${ROOT}/py/server" "${WHEELS[0]}" "${ROOT}/py/client"
# Install the test dependencies one at a time, so a package with no wheel for this Python does not stop the others.
# The tests that need a missing one skip or fail on their own.
while IFS= read -r req; do
    req="${req%%#*}"
    [[ -z "${req// /}" ]] && continue
    uv pip install --quiet --python "${VENV}/bin/python" "${req}" || echo "Skipping ${req}: not installable on ${PY_VERSION}"
done < "${ROOT}/py/${SUITE}/requirements-test.txt"

export VIRTUAL_ENV="${VENV}"
export PATH="${VENV}/bin:${PATH}"
exec "${ROOT}/py/run-tests-venv.sh" "${SUITE}" "$@"
