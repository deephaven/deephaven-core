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
nltk_base_image_id="$(${__gradlew} -q docker-nltk-base:showImageId)"
pytorch_base_image_id="$(${__gradlew} -q docker-pytorch-base:showImageId)"
sklearn_base_image_id="$(${__gradlew} -q docker-sklearn-base:showImageId)"
tensorflow_base_image_id="$(${__gradlew} -q docker-tensorflow-base:showImageId)"
all_ai_base_image_id="$(${__gradlew} -q docker-all-ai-base:showImageId)"

# Write down the (potentially) new requirements
# Need to manually remove pkg-resources
# https://bugs.launchpad.net/ubuntu/+source/python-pip/+bug/1635463
docker run --rm "${server_base_image_id}" pip freeze | grep -v "pkg.resources" > "${__dir}/../../server-jetty/src/main/server-jetty/requirements.txt"
docker run --rm "${server_base_image_id}" pip freeze | grep -v "pkg.resources" > "${__dir}/../../server/src/main/server-netty/requirements.txt"
docker run --rm "${nltk_base_image_id}" pip freeze | grep -v "pkg.resources" > "${__dir}/../../server/src/main/server-nltk-netty/requirements.txt"
docker run --rm "${pytorch_base_image_id}" pip freeze | grep -v "pkg.resources" > "${__dir}/../../server/src/main/server-pytorch-netty/requirements.txt"
docker run --rm "${sklearn_base_image_id}" pip freeze | grep -v "pkg.resources" > "${__dir}/../../server/src/main/server-sklearn-netty/requirements.txt"
docker run --rm "${tensorflow_base_image_id}" pip freeze | grep -v "pkg.resources" > "${__dir}/../../server/src/main/server-tensorflow-netty/requirements.txt"
docker run --rm "${all_ai_base_image_id}" pip freeze | grep -v "pkg.resources" > "${__dir}/../../server/src/main/server-all-ai-netty/requirements.txt"

