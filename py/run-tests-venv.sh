#!/usr/bin/env bash
#
# Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
#

# Runs the Python server or client tests in the active venv, without Docker. The tests run on the venv's Python, with
# its deephaven-core, deephaven-server and pydeephaven.
# - server: runs py/server/tests. test_helper starts the JVM in the test process, on the venv's deephaven-server jars.
#   The Docker image has the same jars in /opt/deephaven/server/lib.
# - client: starts a server with the venv's deephaven-server, then runs py/client/tests against it.
#
# usage: run-tests-venv.sh <server|client> [test ...]
#   test  optional unittest names, for example tests.test_csv. Default: all of tests/.
#
# The venv also needs py/<suite>/requirements-test.txt. The script warns about missing packages and never changes the
# venv. See py/<suite>/TESTING.md.

set -o errexit
set -o nounset
set -o pipefail

if [[ $# -lt 1 || ( "$1" != server && "$1" != client ) ]]; then
    echo "usage: $0 <server|client> [test ...]" >&2
    exit 2
fi
SUITE=$1
shift

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SUITE_DIR="${ROOT}/py/${SUITE}"
DOC="py/${SUITE}/TESTING.md"

if [[ -z "${VIRTUAL_ENV:-}" || ! -x "${VIRTUAL_ENV}/bin/python" ]]; then
    echo "Activate a venv first. See ${DOC}." >&2
    exit 1
fi
PYTHON="${VIRTUAL_ENV}/bin/python"
# The install command for the hints below. A venv made by uv has no pip.
if [[ -x "${VIRTUAL_ENV}/bin/pip" ]]; then
    PIP_INSTALL="pip install"
else
    PIP_INSTALL="uv pip install"
fi
# Look the packages up without importing them. Importing deephaven needs a running JVM.
if ! "${PYTHON}" -c "import importlib.util, sys; sys.exit(not all(importlib.util.find_spec(m) for m in ('deephaven', 'deephaven_server', 'pydeephaven')))"; then
    echo "The venv ${VIRTUAL_ENV} needs deephaven-core, deephaven-server and pydeephaven. See ${DOC}." >&2
    exit 1
fi

# The venv's Python version, with a t suffix for a free-threaded build. It names the build and report folders.
PY_LABEL="$("${PYTHON}" -c 'import sys, sysconfig; print(f"{sys.version_info[0]}.{sys.version_info[1]}" + ("t" if sysconfig.get_config_var("Py_GIL_DISABLED") else ""))')"
BUILD="${SUITE_DIR}/build/test-venv/${PY_LABEL}"
WORK="${BUILD}/work"
RESULTS="${SUITE_DIR}/build/test-results/test-venv/${PY_LABEL}"
rm -rf "${BUILD}" "${RESULTS}"
mkdir -p "${BUILD}/data" "${BUILD}/cache" "${BUILD}/config" "${WORK}" "${RESULTS}"
"${PYTHON}" -VV

# Warn about missing test dependencies. A line with an environment marker is checked only if the packaging library is
# installed to evaluate it.
"${PYTHON}" - "${SUITE_DIR}/requirements-test.txt" "${PIP_INSTALL}" << 'EOF'
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
    print("To install them: " + sys.argv[2] + " -r " + sys.argv[1])
EOF
# The tests run under xmlrunner, the module in the unittest-xml-reporting package, which writes the JUnit XML reports.
# Without it no test can run, so stop here.
if ! "${PYTHON}" -c "import xmlrunner" 2> /dev/null; then
    echo "Stopping: the tests need unittest-xml-reporting. Run: ${PIP_INSTALL} unittest-xml-reporting" >&2
    exit 1
fi

# Deephaven's data, cache and config folders. The server and test_helper read these. Without them, they default to /data
# and /cache, which don't exist outside Docker and need root to create.
export DEEPHAVEN_DATA_DIR="${BUILD}/data"
export DEEPHAVEN_CACHE_DIR="${BUILD}/cache"
export DEEPHAVEN_CONFIG_DIR="${BUILD}/config"
if [[ "${PY_LABEL}" == *t ]]; then
    # grpcio hasn't declared free-thread support, so importing it turns the GIL back on for the rest of the run unless
    # PYTHON_GIL=0.
    export PYTHON_GIL=0
fi

# Each setup function prepares one suite: it sets the suite's environment, copies its tests into WORK, and sets
# DISCOVER to the arguments that find all of its tests.

setup_server_suite() {
    local jars
    jars="$("${PYTHON}" -c 'import importlib.util, os; print(os.path.join(os.path.dirname(importlib.util.find_spec("deephaven_server").origin), "jars"))')"
    if ! compgen -G "${jars}/*.jar" > /dev/null; then
        echo "Found no jars in ${jars}. Reinstall deephaven-server:" >&2
        echo "${PIP_INSTALL} --force-reinstall --no-deps py/embedded-server/build/wheel/deephaven_server-*.whl" >&2
        exit 1
    fi
    export DEEPHAVEN_PROPFILE=dh-defaults.prop
    export DEEPHAVEN_MAXMEM="${DEEPHAVEN_MAXMEM:-4g}"
    # The tests also need the test config, time zones and calendars. They are plain files in the source tree.
    export DEEPHAVEN_CLASSPATH="${ROOT}/props/test-configs/src/main/resources:${ROOT}/Integrations/src/test/resources:${jars}/*"
    # tests/test_time.py expects the JVM default time zone to be named "Etc/UTC", as it is in the Docker image.
    export TZ=Etc/UTC
    # Run from a fresh copy of the tests. They write files such as data_from_dh.parquet into the working directory,
    # and leftovers from an earlier run make some tests fail.
    cp -R "${SUITE_DIR}/tests" "${SUITE_DIR}/test_helper" "${WORK}/"
    DISCOVER=(discover -s tests -t .)
}

# Starts a server from the venv's deephaven-server on port $1 and waits until it is ready. The server stops when this
# script exits.
start_test_server() {
    local port=$1
    # Same settings as the Docker-based testPyClient task: anonymous and PSK logins, PSK "deephaven". Anonymous logins
    # are on, so bind to 127.0.0.1. The server command runs until its stdin closes, so it reads from a FIFO this script
    # holds open. Start it in the build directory: Python puts the working directory first on sys.path, and the
    # repository root's coverage/ folder shadows the coverage package numba imports.
    mkfifo "${BUILD}/server.stdin"
    exec 3<> "${BUILD}/server.stdin"
    (cd "${BUILD}" && exec "${VIRTUAL_ENV}/bin/deephaven" server --host 127.0.0.1 --port "${port}" --no-browser \
        --jvm-args "-Xmx1g -DAuthHandlers=io.deephaven.auth.AnonymousAuthenticationHandler,io.deephaven.authentication.psk.PskAuthenticationHandler -Dauthentication.psk=deephaven -DSystemicObjectTracker.enabled=true" \
        < "${BUILD}/server.stdin" > "${RESULTS}/server.log" 2>&1) &
    SERVER_PID=$!
    # Wait for the server to exit too, so the port is free when this script returns.
    trap 'kill "${SERVER_PID}" 2>/dev/null || true; wait "${SERVER_PID}" 2>/dev/null || true; exec 3>&-' EXIT
    for _ in $(seq 1 120); do
        if grep -q "Deephaven is running at" "${RESULTS}/server.log"; then
            return
        fi
        if ! kill -0 "${SERVER_PID}" 2>/dev/null; then
            echo "The server exited. See ${RESULTS}/server.log." >&2
            exit 1
        fi
        sleep 1
    done
    echo "The server did not start within 120 seconds. See ${RESULTS}/server.log." >&2
    exit 1
}

setup_client_suite() {
    local port=10000
    start_test_server "${port}"
    # Point the tests at this server, whatever DH_HOST and DH_PORT the calling shell has.
    export DH_HOST=127.0.0.1 DH_PORT="${port}"
    # Run from a copy of the tests, so files they write do not land in the source tree.
    cp -R "${SUITE_DIR}/tests" "${WORK}/"
    DISCOVER=(discover tests)
}

if [[ "${SUITE}" == server ]]; then
    setup_server_suite
else
    setup_client_suite
fi

cd "${WORK}"
if [[ $# -gt 0 ]]; then
    "${PYTHON}" -m xmlrunner "$@" -v -o "${RESULTS}"
else
    "${PYTHON}" -m xmlrunner "${DISCOVER[@]}" -v -o "${RESULTS}"
fi
