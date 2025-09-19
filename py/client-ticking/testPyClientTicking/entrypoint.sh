#!/usr/bin/env bash

set -o errexit
set -o pipefail
set -o nounset

# This script is pulled into the docker image that runs the python tests for this project.
# The sole argument says whether to run the tests with coverage (true), or turn coverage
# tracking off (false)

if [[ $# != 1 ]]; then
        echo "$0: endpoint.sh <true | false>"
        exit 1
fi

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

DO_COVERAGE=$1

if [[ "${DO_COVERAGE}" == "true" ]]; then
  coverage run -m xmlrunner discover tests -v -o /out/report
  mkdir -p /out/coverage/
  coverage report > /out/coverage/python-coverage-client-ticking.tsv
  coverage html -d /out/coverage/python-coverage-client-ticking.html
else
  python -m xmlrunner discover tests -v -o /out/report
fi

