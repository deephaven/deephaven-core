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

# Write down the (potentially) new requirements
# Need to manually remove pkg-resources
# https://bugs.launchpad.net/ubuntu/+source/python-pip/+bug/1635463
docker run --rm "${server_base_image_id}" pip freeze | grep -v "pkg.resources" > "${__dir}/../../server-jetty/src/main/server-jetty/requirements.txt"
docker run --rm "${server_base_image_id}" pip freeze | grep -v "pkg.resources" > "${__dir}/../../server/src/main/server-netty/requirements.txt"

