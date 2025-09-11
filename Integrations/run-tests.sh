#!/usr/bin/env bash

set -o errexit
set -o pipefail

# This script is pulled into the docker image that runs the python tests for this project.
# The sole argument says whether to run the tests with coverage (true), or turn coverage
# tracking off (false)

if [[ $# != 1 ]]; then
        echo "$0: run-tests.sh <true | false>"
        exit 1
fi

DO_COVERAGE=$1

if [[ "${DO_COVERAGE}" == "true" ]]; then
  coverage run -m xmlrunner discover -s tests -t . -v -o /out/report
  mkdir -p /out/coverage/
  coverage report > /out/coverage/python-coverage.tsv
else
  python3 -m xmlrunner discover -s tests -t . -v -o /out/report
fi
