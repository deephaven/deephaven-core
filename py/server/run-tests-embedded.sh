#!/usr/bin/env bash
#
# Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
#

# Runs the tests in tests/ in the active venv, without the server Docker image. The JVM comes from the deephaven-server
# package in the venv, so the tests run on that venv's Python and on whichever Deephaven packages it has.
#
# usage: run-tests-embedded.sh [test ...]
#   test  optional unittest names, for example tests.test_csv. Default: all of tests/.
#
# Before the first run:
#   1. Build the Java test classes: ./gradlew :Integrations:testClasses :engine-time:testClasses
#   2. Activate a venv that has deephaven-core, deephaven-server and pydeephaven. TESTING.md shows how to make one.
# The script installs any package from requirements-test.txt that the venv lacks. It never changes the packages the
# venv already has. It needs uv.
#
# See TESTING.md for details, including the Kafka and Postgres tests.

set -o errexit
set -o nounset
set -o pipefail

SERVER_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "${SERVER_DIR}/../.." && pwd)"

if ! command -v uv > /dev/null; then
    echo "This script needs uv. See https://docs.astral.sh/uv/getting-started/installation/" >&2
    exit 1
fi
if [[ -z "${VIRTUAL_ENV:-}" || ! -x "${VIRTUAL_ENV}/bin/python" ]]; then
    echo "Activate a venv first. See py/server/TESTING.md." >&2
    exit 1
fi
PYTHON="${VIRTUAL_ENV}/bin/python"
# Look the packages up without importing them. Importing deephaven needs a running JVM.
if ! "${PYTHON}" -c "import importlib.util, sys; sys.exit(not all(importlib.util.find_spec(m) for m in ('deephaven', 'deephaven_server', 'pydeephaven')))"; then
    echo "The venv ${VIRTUAL_ENV} needs deephaven-core, deephaven-server and pydeephaven. See py/server/TESTING.md." >&2
    exit 1
fi
for dir in "${ROOT}/Integrations/build/classes/java/test" "${ROOT}/engine/time/build/classes/java/test"; do
    if [[ ! -d "${dir}" ]]; then
        echo "Missing ${dir}. Run: ./gradlew :Integrations:testClasses :engine-time:testClasses" >&2
        exit 1
    fi
done

# The Python version, such as 3.12 or 3.14t for a free-threaded build. It names the build and report folders.
PY_LABEL="$("${PYTHON}" -c 'import sys, sysconfig; print(f"{sys.version_info[0]}.{sys.version_info[1]}" + ("t" if sysconfig.get_config_var("Py_GIL_DISABLED") else ""))')"
BUILD="${SERVER_DIR}/build/test-embedded/${PY_LABEL}"
WORK="${BUILD}/work"
RESULTS="${SERVER_DIR}/build/test-results/test-embedded/${PY_LABEL}"
rm -rf "${BUILD}" "${RESULTS}"
mkdir -p "${BUILD}/data" "${BUILD}/cache" "${WORK}" "${RESULTS}"
"${PYTHON}" -VV

# Install the test dependencies the venv lacks. The packages it already has are passed as constraints, so an install
# never upgrades or downgrades them. A package that cannot install, for example one with no wheel for this Python, is
# skipped. The tests that need it then skip or fail on their own.
uv pip freeze --python "${PYTHON}" > "${BUILD}/before.txt"
grep -E '^[A-Za-z0-9._-]+==' "${BUILD}/before.txt" > "${BUILD}/constraints.txt" || true
while IFS= read -r req; do
    req="${req%%#*}"
    [[ -z "${req// /}" ]] && continue
    uv pip install --quiet --python "${PYTHON}" -c "${BUILD}/constraints.txt" "${req}" \
        || echo "Could not install ${req}. The tests that need it will skip or fail."
done < "${SERVER_DIR}/requirements-test.txt"
uv pip freeze --python "${PYTHON}" | grep -v -x -F -f "${BUILD}/before.txt" | sed 's/^/Installed test dependency: /' || true

# Run from a fresh copy of the tests. They write files such as data_from_dh.parquet into the working directory, and
# leftovers from an earlier run make some tests fail.
cp -R "${SERVER_DIR}/tests" "${SERVER_DIR}/test_helper" "${WORK}/"

export DEEPHAVEN_TEST_EMBEDDED=true
export DEEPHAVEN_PROPFILE=dh-defaults.prop
export DEEPHAVEN_MAXMEM="${DEEPHAVEN_MAXMEM:-4g}"
export DEEPHAVEN_DATA_DIR="${BUILD}/data"
export DEEPHAVEN_CACHE_DIR="${BUILD}/cache"
# Beyond the server jars, the tests need the test config, time zones and calendars, and three Java test classes:
# CalendarInit (engine-time), and MyObject and PythonTimeComponentsTest (Integrations).
export DEEPHAVEN_CLASSPATH="${ROOT}/props/test-configs/src/main/resources:${ROOT}/Integrations/src/test/resources:${ROOT}/Integrations/build/classes/java/test:${ROOT}/engine/time/build/classes/java/test"
# tests/test_time.py expects the JVM default time zone to be named "Etc/UTC", as it is in the Docker image.
export TZ=Etc/UTC
if [[ "${PY_LABEL}" == *t ]]; then
    # grpcio, which pydeephaven and test_barrage use, has not declared itself free-thread safe. Importing it turns
    # the GIL back on for the rest of the run, unless PYTHON_GIL=0 keeps it off.
    export PYTHON_GIL=0
fi

cd "${WORK}"
if [[ $# -gt 0 ]]; then
    "${PYTHON}" -m xmlrunner "$@" -v -o "${RESULTS}"
else
    "${PYTHON}" -m xmlrunner discover -s tests -t . -v -o "${RESULTS}"
fi
