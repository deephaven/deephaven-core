#!/usr/bin/env bash
set -o errexit
set -o pipefail
set -o nounset
set -o xtrace

# This script is a helper utility to automate the process of bumping the base images and updated the requirements.

__dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
__gradlew="${__dir}/../../../gradlew"

# Pull and update the registry images
"${__gradlew}" pullImage bumpImage --continue

# Get the (potentially) new IDs
server_base_image_id="$(${__gradlew} -q docker-server-base:showImageId)"

# The behavior of pip freeze has changed between Python 3.10 and Python 3.12, and now includes wheel and setuptools
# versions in the output. While we shouldn't technically need to preserve this behavior, we'll continue excluding them
# from the output here.
docker run --rm "${server_base_image_id}" \
  pip freeze \
  --exclude wheel \
  --exclude setuptools \
  | tee \
  "${__dir}/../../server-jetty/src/main/server-jetty/requirements.txt" \
  "${__dir}/../../server/src/main/server-netty/requirements.txt"
