#!/usr/bin/env bash
#
# Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
#

# Runs the tests in tests/ in the active venv, against the server-jetty distribution instead of the server Docker
# image. The distribution has the same start script and jars as the image. Java is the main program and hosts Python
# from the same venv, so the server and the client both run on that venv's Python.
#
# usage: run-tests-venv.sh [test ...]
#   test  optional unittest names, for example tests.test_session. Default: all of tests/.
#
# Before the first run:
#   1. Build the server distribution: ./gradlew :server-jetty-app:installDist
#   2. Activate a venv that has pydeephaven and deephaven-core. py/client/TESTING.md shows how to make one.
# The venv also needs the packages in requirements-test.txt. The script warns about any that are missing. It never
# installs or changes anything in the venv.
#
# See py/client/TESTING.md for details.

set -o errexit
set -o nounset
set -o pipefail

CLIENT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "${CLIENT_DIR}/../.." && pwd)"
PORT=10000
SERVER_DIST="${ROOT}/server/jetty-app/build/install/server-jetty"

if [[ -z "${VIRTUAL_ENV:-}" || ! -x "${VIRTUAL_ENV}/bin/python" ]]; then
    echo "Activate a venv first. See py/client/TESTING.md." >&2
    exit 1
fi
PYTHON="${VIRTUAL_ENV}/bin/python"
# Look the packages up without importing them. Importing deephaven needs a running JVM.
if ! "${PYTHON}" -c "import importlib.util, sys; sys.exit(not all(importlib.util.find_spec(m) for m in ('deephaven', 'pydeephaven')))"; then
    echo "The venv ${VIRTUAL_ENV} needs pydeephaven and deephaven-core. See py/client/TESTING.md." >&2
    exit 1
fi
if [[ ! -x "${SERVER_DIST}/bin/start" ]]; then
    echo "Missing the server distribution in ${SERVER_DIST}. Run: ./gradlew :server-jetty-app:installDist" >&2
    exit 1
fi

# The Python version, such as 3.12 or 3.14t for a free-threaded build. It names the build and report folders.
PY_LABEL="$("${PYTHON}" -c 'import sys, sysconfig; print(f"{sys.version_info[0]}.{sys.version_info[1]}" + ("t" if sysconfig.get_config_var("Py_GIL_DISABLED") else ""))')"
BUILD="${CLIENT_DIR}/build/test-venv/${PY_LABEL}"
WORK="${BUILD}/work"
RESULTS="${CLIENT_DIR}/build/test-results/test-venv/${PY_LABEL}"
rm -rf "${BUILD}" "${RESULTS}"
mkdir -p "${BUILD}/data" "${BUILD}/cache" "${BUILD}/config" "${WORK}" "${RESULTS}"
"${PYTHON}" -VV

# Warn about test dependencies the venv lacks. The tests that need one skip or fail on their own. A line with an
# environment marker is checked only when the packaging library is there to evaluate the marker.
"${PYTHON}" - "${CLIENT_DIR}/requirements-test.txt" << 'EOF'
import importlib.metadata, re, sys
missing = []
for line in open(sys.argv[1]):
    line = line.split("#")[0].strip()
    if not line:
        continue
    req, _, marker = line.partition(";")
    if marker.strip():
        try:
            from packaging.markers import Marker
        except ImportError:
            continue
        if not Marker(marker).evaluate():
            continue
    name = re.split(r"[\s\[<>=!~]", req.strip())[0]
    try:
        importlib.metadata.distribution(name)
    except importlib.metadata.PackageNotFoundError:
        missing.append(name)
if missing:
    print("Missing test dependencies: " + ", ".join(missing) + ". The tests that need them will skip or fail.")
    print("To install them: pip install -r " + sys.argv[1])
EOF
if ! "${PYTHON}" -c "import xmlrunner" 2> /dev/null; then
    echo "The venv needs unittest-xml-reporting to run the tests. Run: pip install -r ${CLIENT_DIR}/requirements-test.txt" >&2
    exit 1
fi

if [[ "${PY_LABEL}" == *t ]]; then
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
cp -R "${CLIENT_DIR}/tests" "${WORK}/"
cd "${WORK}"
if [[ $# -gt 0 ]]; then
    "${PYTHON}" -m xmlrunner "$@" -v -o "${RESULTS}"
else
    "${PYTHON}" -m xmlrunner discover tests -v -o "${RESULTS}"
fi
