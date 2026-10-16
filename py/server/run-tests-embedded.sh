#!/usr/bin/env bash
#
# Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
#

# Runs the tests in tests/ on one Python version, without the server Docker image. The JVM comes from the
# deephaven-server wheel. uv creates the venv and downloads the Python version if it is not installed.
#
# usage: run-tests-embedded.sh <python-version> [test ...]
#   python-version  any version uv accepts, for example 3.9, 3.14 or 3.14t (free-threaded)
#   test            optional unittest names, for example tests.test_csv. Default: all of tests/.
#
# Build the prerequisites first, or run it through Gradle, which builds them:
#   ./gradlew :py-embedded-server:assemble :Integrations:testClasses :engine-time:testClasses
#   ./gradlew :py-server:testEmbedded -PpythonVersion=3.14t
#
# See TESTING.md for details, including the Kafka and Postgres tests.

set -o errexit
set -o nounset
set -o pipefail

if [[ $# -lt 1 ]]; then
    echo "usage: $0 <python-version> [test ...]" >&2
    exit 2
fi
PY_VERSION=$1
shift

SERVER_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "${SERVER_DIR}/../.." && pwd)"
BUILD="${SERVER_DIR}/build/test-embedded/${PY_VERSION}"
VENV="${BUILD}/venv"
WORK="${BUILD}/work"
RESULTS="${SERVER_DIR}/build/test-results/test-embedded/${PY_VERSION}"

shopt -s nullglob
WHEELS=("${ROOT}"/py/embedded-server/build/wheel/deephaven_server-*.whl)
shopt -u nullglob
if [[ ${#WHEELS[@]} -ne 1 ]]; then
    echo "Expected one deephaven-server wheel in py/embedded-server/build/wheel, found ${#WHEELS[@]}." >&2
    echo "Run: ./gradlew :py-embedded-server:assemble" >&2
    exit 1
fi
for dir in "${ROOT}/Integrations/build/classes/java/test" "${ROOT}/engine/time/build/classes/java/test"; do
    if [[ ! -d "${dir}" ]]; then
        echo "Missing ${dir}. Run: ./gradlew :Integrations:testClasses :engine-time:testClasses" >&2
        exit 1
    fi
done

# py/server/setup.py and py/client/setup.py read their version from DEEPHAVEN_VERSION. It must match the wheel's
# version, because deephaven-server pins deephaven-core to it.
DEEPHAVEN_VERSION="$(basename "${WHEELS[0]}" | cut -d- -f2)"
export DEEPHAVEN_VERSION

rm -rf "${BUILD}" "${RESULTS}"
mkdir -p "${BUILD}" "${RESULTS}" "${BUILD}/data" "${BUILD}/cache"
# --managed-python makes uv use its own Python build, which it downloads at the newest patch release it knows. Without
# it, uv takes any matching Python already installed, which can be many patch releases old.
uv venv --quiet --managed-python --python "${PY_VERSION}" "${VENV}"
"${VENV}/bin/python" -VV
# --reinstall-package rebuilds the local packages on every run. Without it, uv can reuse a cached build of older source,
# because the package version does not change between edits.
uv pip install --quiet --python "${VENV}/bin/python" --reinstall-package deephaven-core --reinstall-package pydeephaven \
    "${ROOT}/py/server" "${WHEELS[0]}" "${ROOT}/py/client" unittest-xml-reporting
# Soft dependencies. Some have no wheel for every Python version, for example numba and connectorx on 3.13t. Their
# tests skip or fail on their own when the package is missing, so a missing one must not stop the run.
for pkg in numba adbc-driver-manager adbc-driver-postgresql connectorx; do
    uv pip install --quiet --python "${VENV}/bin/python" "${pkg}" || echo "Skipping ${pkg}: not installable on ${PY_VERSION}"
done

# Run from a fresh copy of the tests. They write files such as data_from_dh.parquet into the working directory, and
# leftovers from an earlier run make some tests fail.
mkdir -p "${WORK}"
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
if [[ "${PY_VERSION}" == *t ]]; then
    # grpcio, which pydeephaven and test_barrage use, has not declared itself free-thread safe. Importing it turns
    # the GIL back on for the rest of the run, unless PYTHON_GIL=0 keeps it off.
    export PYTHON_GIL=0
fi

cd "${WORK}"
if [[ $# -gt 0 ]]; then
    "${VENV}/bin/python" -m xmlrunner "$@" -v -o "${RESULTS}"
else
    "${VENV}/bin/python" -m xmlrunner discover -s tests -t . -v -o "${RESULTS}"
fi
