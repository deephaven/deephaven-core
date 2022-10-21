#!/bin/bash
set -o errexit
set -o pipefail
set -o nounset
set -o xtrace

XML="${1}";

./tests --reporter XML --out "$XML";
