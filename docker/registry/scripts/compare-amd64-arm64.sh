#!/usr/bin/env bash
set -o errexit
set -o pipefail
set -o nounset
set -o xtrace

# This script is a helper utility to ensure that the python dependencies on amd64 and arm64 versions match, and to help
# update the requirements.txt files. It is not automatically run. Typically, this script should be run after
# `./gradlew pullImage bumpImage --continue`.

__dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
__gradlew="${__dir}/../../../gradlew"

server_base_image_id="$(${__gradlew} -q docker-server-base:showImageId)"

podman run -q --rm --override-arch arm64 "${server_base_image_id}" pip freeze > build/server-base.arm64.txt
podman run -q --rm --override-arch amd64 "${server_base_image_id}" pip freeze > build/server-base.amd64.txt
diff -q build/server-base.amd64.txt build/server-base.arm64.txt
