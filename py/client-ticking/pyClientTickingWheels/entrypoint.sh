#!/usr/bin/env bash

set -o errexit
set -o pipefail
set -o nounset

cd "${PREFIX}/src/py-client-ticking"
source "${PREFIX}/env.sh"
ORIG_PATH="$PATH"
for spec in ${WHEELS_SET}; do
  tag=$(echo "$spec" | cut -d: -f 2)
  rm -f ./*.cpp ./*.so
  PATH="/opt/python/${tag}/bin:$ORIG_PATH"
  pip3 install cython
  MAKEFLAGS="-j${NCPUS}" \
    CFLAGS="-I${DHCPP}/include" \
    LDFLAGS="-L${DHCPP}/lib" \
    DEEPHAVEN_VERSION="${DEEPHAVEN_VERSION}" \
    python3 setup.py build_ext -i
    DEEPHAVEN_VERSION="${DEEPHAVEN_VERSION}" python3 setup.py bdist_wheel
    auditwheel repair --wheel-dir /out dist/pydeephaven_ticking*"${tag}"*.whl
    rm -f dist/pydeephaven_ticking*"${tag}"*.whl
done
