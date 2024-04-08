#!/usr/bin/env bash

set -o errexit
set -o pipefail
set -o nounset

source "/project/${PYTHON_VERSION}/bin/activate"

# Ensure that pydeephaven and pydeephaven-ticking are resolved from our built wheels and _not_ PyPi.
pip install \
  --only-binary=":all:" \
  --find-links=/project/dep-wheels \
  --find-links=/project/pyt-wheels \
  --no-index \
  --no-deps \
  pydeephaven pydeephaven-ticking

# Resolve transitive dependencies from PyPi
pip install \
  --only-binary=":all:" \
  --find-links=/project/dep-wheels \
  --find-links=/project/pyt-wheels \
  pydeephaven pydeephaven-ticking

python -m xmlrunner discover tests -v -o /out/report/

