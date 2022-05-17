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
nltk_base_image_id="$(${__gradlew} -q docker-nltk-base:showImageId)"
pytorch_base_image_id="$(${__gradlew} -q docker-pytorch-base:showImageId)"
sklearn_base_image_id="$(${__gradlew} -q docker-sklearn-base:showImageId)"
tensorflow_base_image_id="$(${__gradlew} -q docker-tensorflow-base:showImageId)"

podman run -q --rm --override-arch arm64 "${server_base_image_id}" pip freeze > build/server-base.arm64.txt
podman run -q --rm --override-arch amd64 "${server_base_image_id}" pip freeze > build/server-base.amd64.txt
diff -q build/server-base.amd64.txt build/server-base.arm64.txt

podman run -q --rm --override-arch arm64 "${nltk_base_image_id}" pip freeze > build/nltk-base.arm64.txt
podman run -q --rm --override-arch amd64 "${nltk_base_image_id}" pip freeze > build/nltk-base.amd64.txt
diff -q build/nltk-base.amd64.txt build/nltk-base.arm64.txt

podman run -q --rm --override-arch arm64 "${pytorch_base_image_id}" pip freeze > build/pytorch-base.arm64.txt
podman run -q --rm --override-arch amd64 "${pytorch_base_image_id}" pip freeze > build/pytorch-base.amd64.txt
diff -q build/pytorch-base.amd64.txt build/pytorch-base.arm64.txt

podman run -q --rm --override-arch arm64 "${sklearn_base_image_id}" pip freeze > build/sklearn-base.arm64.txt
podman run -q --rm --override-arch amd64 "${sklearn_base_image_id}" pip freeze > build/sklearn-base.amd64.txt
diff -q build/sklearn-base.amd64.txt build/sklearn-base.arm64.txt

podman run -q --rm --override-arch arm64 "${tensorflow_base_image_id}" pip freeze > build/tensorflow-base.arm64.txt
podman run -q --rm --override-arch amd64 "${tensorflow_base_image_id}" pip freeze > build/tensorflow-base.amd64.txt
diff -q build/tensorflow-base.amd64.txt build/tensorflow-base.arm64.txt
