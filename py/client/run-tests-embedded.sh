#!/usr/bin/env bash
#
# Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
#

# Runs the tests in tests/ on one Python version, against the server-jetty distribution instead of the server Docker
# image. The distribution has the same start script and jars as the image. Java is the main program and hosts Python.
# The server and the client share one venv, so both run on that Python version. uv creates the venv and downloads the
# Python version if it is not installed.
#
# usage: run-tests-embedded.sh <python-version> [test ...]
#   python-version  any version uv accepts, for example 3.9, 3.14 or 3.14t (free-threaded)
#   test            optional unittest names, for example tests.test_session. Default: all of tests/.
#
# Build the server distribution first, or run it through Gradle, which builds it:
#   ./gradlew :server-jetty-app:installDist
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

SERVER_DIST="${ROOT}/server/jetty-app/build/install/server-jetty"
shopt -s nullglob
ENGINE_JARS=("${SERVER_DIST}"/lib/deephaven-engine-table-*.jar)
shopt -u nullglob
if [[ ! -x "${SERVER_DIST}/bin/start" || ${#ENGINE_JARS[@]} -ne 1 ]]; then
    echo "Missing the server distribution in ${SERVER_DIST}. Run: ./gradlew :server-jetty-app:installDist" >&2
    exit 1
fi

# py/server/setup.py and py/client/setup.py read their version from DEEPHAVEN_VERSION. Use the server's version, taken
# from one of its jar names, for example deephaven-engine-table-43.0-SNAPSHOT.jar.
DEEPHAVEN_VERSION="$(basename "${ENGINE_JARS[0]}" .jar)"
DEEPHAVEN_VERSION="${DEEPHAVEN_VERSION#deephaven-engine-table-}"
export DEEPHAVEN_VERSION

rm -rf "${BUILD}" "${RESULTS}"
mkdir -p "${BUILD}" "${RESULTS}" "${BUILD}/data" "${BUILD}/cache" "${BUILD}/config"
# --managed-python makes uv use its own Python build, which it downloads at the newest patch release it knows. Without
# it, uv takes any matching Python already installed, which can be many patch releases old.
uv venv --quiet --managed-python --python "${PY_VERSION}" "${VENV}"
"${VENV}/bin/python" -VV
# --reinstall-package rebuilds the local packages on every run. Without it, uv can reuse a cached build of older source,
# because the package version does not change between edits.
uv pip install --quiet --python "${VENV}/bin/python" --reinstall-package deephaven-core --reinstall-package pydeephaven \
    "${ROOT}/py/server" "${ROOT}/py/client" unittest-xml-reporting
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

# Start the server with the settings of the Docker-based testPyClient task: anonymous and PSK logins, PSK "deephaven".
# Anonymous logins are on, so it listens on 127.0.0.1 only. The start script finds Python through VIRTUAL_ENV. The
# server reads its directories from DEEPHAVEN_*_DIR, as in the Docker image.
# Start it from the build directory. Python puts the working directory first on sys.path, and the repository root has a
# coverage/ folder that shadows the coverage package numba imports.
cd "${BUILD}"
VIRTUAL_ENV="${VENV}" \
    DEEPHAVEN_DATA_DIR="${BUILD}/data" DEEPHAVEN_CACHE_DIR="${BUILD}/cache" DEEPHAVEN_CONFIG_DIR="${BUILD}/config" \
    START_OPTS="-Xmx1g -DAuthHandlers=io.deephaven.auth.AnonymousAuthenticationHandler,io.deephaven.authentication.psk.PskAuthenticationHandler -Dauthentication.psk=deephaven -DSystemicObjectTracker.enabled=true -Dhttp.host=127.0.0.1 -Dhttp.port=${PORT}" \
    "${SERVER_DIST}/bin/start" > "${RESULTS}/server.log" 2>&1 &
SERVER_PID=$!
# Wait for the server to exit too, so the port is free when this script returns.
trap 'kill "${SERVER_PID}" 2>/dev/null || true; wait "${SERVER_PID}" 2>/dev/null || true' EXIT

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

# Point the tests at this server, whatever DH_HOST and DH_PORT the calling shell has.
export DH_HOST=127.0.0.1 DH_PORT="${PORT}"

# Run from a copy of the tests, so files they write do not land in the source tree.
mkdir -p "${WORK}"
cp -R "${CLIENT_DIR}/tests" "${WORK}/"
cd "${WORK}"
if [[ $# -gt 0 ]]; then
    "${VENV}/bin/python" -m xmlrunner "$@" -v -o "${RESULTS}"
else
    "${VENV}/bin/python" -m xmlrunner discover tests -v -o "${RESULTS}"
fi
