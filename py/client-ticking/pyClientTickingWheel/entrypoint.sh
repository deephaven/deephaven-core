#!/usr/bin/env bash

set -o errexit
set -o pipefail
set -o nounset

cd "${PREFIX}/src/py-client-ticking"
source "${PREFIX}/env.sh"

rm -f ./*.cpp ./*.so

# Note: using PATH b/c these do not have venv activates
PATH="/opt/python/${PYTHON_TAG}/bin:$PATH"

MAKEFLAGS="-j${NCPUS}" \
  CPPFLAGS="-I${DHCPP}/include" \
  LDFLAGS="-L${DHCPP}/lib" \
  DEEPHAVEN_VERSION="${DEEPHAVEN_VERSION}" \
  python setup.py build_ext -i

DEEPHAVEN_VERSION="${DEEPHAVEN_VERSION}" \
  python setup.py bdist_wheel

auditwheel repair --wheel-dir /out dist/pydeephaven_ticking*"${PYTHON_TAG}"*.whl
