#!/usr/bin/env bash

set -o errexit
set -o pipefail

DO_COVERAGE=$1

echo "Coverage Enabled = ${DO_COVERAGE}" > /out/report/enabled.txt

if [[ "${DO_COVERAGE}" == "true" ]]; then
  coverage run -m xmlrunner discover tests -v -o /out/report
  mkdir -p /out/coverage/
  coverage report > /out/coverage/python-coverage.tsv
else
  python -m xmlrunner discover tests -v -o /out/report
fi