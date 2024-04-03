#!/usr/bin/env bash

set -o errexit
set -o pipefail
set -o nounset

for spec in ${WHEELS_SET}; do
  pyver=$(echo "$spec" | cut -d: -f 1)
  tag=$(echo "$spec" | cut -d: -f 2)
  [ -f /project/pyt-wheels/pydeephaven_ticking*"${tag}"*.whl ]
  source "/project/$pyver/bin/activate"
  pip install unittest-xml-reporting
  pip install /project/dep-wheels/*.whl
  pip install /project/pyt-wheels/pydeephaven_ticking*"${tag}"*.whl
  python -m xmlrunner discover tests -v -o "/out/report/$pyver"
  deactivate
  rm -r "/project/$pyver"
done
