#!/usr/bin/env bash

set -o errexit
set -o pipefail
set -o nounset

# Runs the Python server or client tests on one Python version, for the Nightly Python Versions CI workflow. It makes a
# fresh venv, installs the Deephaven packages and the test dependencies, then calls py/<suite>/run-tests-embedded.sh.
# Developers who want to run the tests in their own venv call that script directly, see py/server/TESTING.md.
#
# usage: py-versions-test.sh <server|client> <python-version> [test ...]
#   python-version  any version uv accepts, for example 3.9, 3.14 or 3.14t (free-threaded)
#   test            optional unittest names, passed on to run-tests-embedded.sh. Default: all of tests/.
#
# Needs uv and the build outputs: ./gradlew :py-embedded-server:assemble :Integrations:testClasses
# :engine-time:testClasses for the server tests, ./gradlew :server-jetty-app:installDist for the client tests.

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
if [[ "${SUITE}" == server ]]; then
    WHEELS=("${ROOT}"/py/embedded-server/build/wheel/deephaven_server-*.whl)
    if [[ ${#WHEELS[@]} -ne 1 ]]; then
        echo "Expected one deephaven-server wheel in py/embedded-server/build/wheel, found ${#WHEELS[@]}." >&2
        exit 1
    fi
    # py/server/setup.py and py/client/setup.py read their version from DEEPHAVEN_VERSION. It must match the wheel's
    # version, because deephaven-server pins deephaven-core to it.
    DEEPHAVEN_VERSION="$(basename "${WHEELS[0]}" | cut -d- -f2)"
    PACKAGES=("${ROOT}/py/server" "${WHEELS[0]}" "${ROOT}/py/client")
else
    ENGINE_JARS=("${ROOT}"/server/jetty-app/build/install/server-jetty/lib/deephaven-engine-table-*.jar)
    if [[ ${#ENGINE_JARS[@]} -ne 1 ]]; then
        echo "Missing the server distribution. Run: ./gradlew :server-jetty-app:installDist" >&2
        exit 1
    fi
    # Use the server's version, taken from one of its jar names, for example deephaven-engine-table-43.0-SNAPSHOT.jar.
    DEEPHAVEN_VERSION="$(basename "${ENGINE_JARS[0]}" .jar)"
    DEEPHAVEN_VERSION="${DEEPHAVEN_VERSION#deephaven-engine-table-}"
    PACKAGES=("${ROOT}/py/server" "${ROOT}/py/client")
fi
shopt -u nullglob
export DEEPHAVEN_VERSION

rm -rf "${VENV}"
# --managed-python makes uv use its own Python build, which it downloads at the newest patch release it knows. Without
# it, uv takes any matching Python already installed, which can be many patch releases old.
uv venv --quiet --managed-python --python "${PY_VERSION}" "${VENV}"
# --reinstall-package rebuilds the local packages. Without it, uv can reuse a cached build of older source, because the
# package version does not change between edits.
uv pip install --quiet --python "${VENV}/bin/python" --reinstall-package deephaven-core --reinstall-package pydeephaven \
    "${PACKAGES[@]}"
# Install the test dependencies here, one at a time and without constraints, so uv may adjust the packages above to fit
# them. run-tests-embedded.sh would only add packages that fit the venv as it is. A missing one must not stop the run.
while IFS= read -r req; do
    req="${req%%#*}"
    [[ -z "${req// /}" ]] && continue
    uv pip install --quiet --python "${VENV}/bin/python" "${req}" || echo "Skipping ${req}: not installable on ${PY_VERSION}"
done < "${ROOT}/py/${SUITE}/requirements-test.txt"

export VIRTUAL_ENV="${VENV}"
export PATH="${VENV}/bin:${PATH}"
exec "${ROOT}/py/${SUITE}/run-tests-embedded.sh" "$@"
