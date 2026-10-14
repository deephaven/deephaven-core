#!/usr/bin/env bash
#
# Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
#

# Runs the tests in tests/ on one Python version, against a server started from the deephaven-server wheel instead of
# the server Docker image. The server and the client share one venv, so both run on that Python version. uv creates
# the venv and downloads the Python version if it is not installed.
#
# usage: run-tests-embedded.sh <python-version> [test ...]
#   python-version  any version uv accepts, for example 3.9, 3.14 or 3.14t (free-threaded)
#   test            optional unittest names, for example tests.test_session. Default: all of tests/.
#
# Build the deephaven-server wheel first, or run it through Gradle, which builds it:
#   ./gradlew :py-embedded-server:assemble
#   ./gradlew :py-client:testEmbedded -PpythonVersion=3.14t
#
# See py/server/TESTING.md for details.

set -o errexit
set -o nounset
set -o pipefail

if [[ $# -lt 1 ]]; then
    echo "usage: $0 <python-version> [test ...]" >&2
    exit 2
fi
PY_VERSION=$1
shift

CLIENT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "${CLIENT_DIR}/../.." && pwd)"
BUILD="${CLIENT_DIR}/build/test-embedded/${PY_VERSION}"
VENV="${BUILD}/venv"
WORK="${BUILD}/work"
RESULTS="${CLIENT_DIR}/build/test-results/test-embedded/${PY_VERSION}"
PORT=10000

shopt -s nullglob
WHEELS=("${ROOT}"/py/embedded-server/build/wheel/deephaven_server-*.whl)
shopt -u nullglob
if [[ ${#WHEELS[@]} -ne 1 ]]; then
    echo "Expected one deephaven-server wheel in py/embedded-server/build/wheel, found ${#WHEELS[@]}." >&2
    echo "Run: ./gradlew :py-embedded-server:assemble" >&2
    exit 1
fi

# py/server/setup.py and py/client/setup.py read their version from DEEPHAVEN_VERSION. It must match the wheel's
# version, because deephaven-server pins deephaven-core to it.
DEEPHAVEN_VERSION="$(basename "${WHEELS[0]}" | cut -d- -f2)"
export DEEPHAVEN_VERSION

rm -rf "${BUILD}" "${RESULTS}"
mkdir -p "${BUILD}" "${RESULTS}" "${BUILD}/data" "${BUILD}/cache"
uv venv --quiet --python "${PY_VERSION}" "${VENV}"
uv pip install --quiet --python "${VENV}/bin/python" \
    "${ROOT}/py/server" "${WHEELS[0]}" "${ROOT}/py/client" unittest-xml-reporting
# Soft dependencies. tests/test_flightsql.py uses flightsql-dbapi and adbc-driver-flightsql, and the server script in
# tests/test_console.py uses numba, as the server Docker image has it. A missing one must not stop the other tests.
for pkg in flightsql-dbapi adbc-driver-flightsql numba; do
    uv pip install --quiet --python "${VENV}/bin/python" "${pkg}" || echo "Skipping ${pkg}: not installable on ${PY_VERSION}"
done

if [[ "${PY_VERSION}" == *t ]]; then
    # grpcio, which pydeephaven uses, has not declared itself free-thread safe. Importing it turns the GIL back on,
    # unless PYTHON_GIL=0 keeps it off.
    export PYTHON_GIL=0
fi

# Start the server the same way the Docker-based testPyClient task does: anonymous and PSK logins, PSK "deephaven".
# Its "server" command exits when stdin closes, so stdin is a FIFO that this script holds open until it exits.
FIFO="${BUILD}/server-stdin"
mkfifo "${FIFO}"
exec 3<>"${FIFO}"
# Start it from the build directory. Python puts the working directory first on sys.path, and the repository root
# has a coverage/ folder that shadows the coverage package numba imports.
cd "${BUILD}"
"${VENV}/bin/python" -c "from deephaven_server.cli import cli; cli()" server \
    --port "${PORT}" --no-browser \
    --jvm-args "-Xmx1g -DAuthHandlers=io.deephaven.auth.AnonymousAuthenticationHandler,io.deephaven.authentication.psk.PskAuthenticationHandler -Dauthentication.psk=deephaven -DSystemicObjectTracker.enabled=true -Ddeephaven.dataDir=${BUILD}/data -Ddeephaven.cacheDir=${BUILD}/cache" \
    <&3 > "${RESULTS}/server.log" 2>&1 &
SERVER_PID=$!
trap 'kill "${SERVER_PID}" 2>/dev/null || true; exec 3>&-' EXIT

for _ in $(seq 1 120); do
    if grep -q "Server started on port ${PORT}" "${RESULTS}/server.log"; then
        break
    fi
    if ! kill -0 "${SERVER_PID}" 2>/dev/null; then
        echo "The server exited. See ${RESULTS}/server.log." >&2
        exit 1
    fi
    sleep 1
done
if ! grep -q "Server started on port ${PORT}" "${RESULTS}/server.log"; then
    echo "The server did not start within 120 seconds. See ${RESULTS}/server.log." >&2
    exit 1
fi

# Run from a copy of the tests, so files they write do not land in the source tree.
mkdir -p "${WORK}"
cp -R "${CLIENT_DIR}/tests" "${WORK}/"
cd "${WORK}"
if [[ $# -gt 0 ]]; then
    "${VENV}/bin/python" -m xmlrunner "$@" -v -o "${RESULTS}"
else
    "${VENV}/bin/python" -m xmlrunner discover tests -v -o "${RESULTS}"
fi
