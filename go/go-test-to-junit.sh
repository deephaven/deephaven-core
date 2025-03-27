#!/bin/bash
set -o errexit
set -o pipefail
set -o nounset
set -o xtrace

XML=${1};

go test -vet=all -v ./... 2>&1 | go-junit-report -set-exit-code -iocopy -out "$XML";
