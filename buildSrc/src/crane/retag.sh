#!/usr/bin/env bash

set -o errexit
set -o pipefail
set -o nounset

crane tag ${imageId} v${version}
