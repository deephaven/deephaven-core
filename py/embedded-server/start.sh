#!/usr/bin/env bash

set -o errexit
set -o pipefail
set -o nounset

JAVA_HOME="${JAVA_HOME}"
PYTHON="${PYTHON:-python3}"
VENV_PATH="/tmp/py-embedded-server-${PYTHON}"
PORT="${PORT:-8080}"

#./gradlew py-server:assemble py-embedded-server:assemble

"${PYTHON}" -m venv "${VENV_PATH}"

"${VENV_PATH}/bin/pip" install -q --upgrade pip setuptools
"${VENV_PATH}/bin/pip" install -q -r docker/server/src/main/server/requirements.txt
"${VENV_PATH}/bin/pip" install -q \
  py/server/build/wheel/deephaven_core-0.14.0.dev2-py3-none-any.whl \
  py/embedded-server/build/wheel/deephaven_server-0.14.0.dev2-py3-none-any.whl

"${VENV_PATH}/bin/python" -i <(cat <<EOF
from deephaven_server import *
server = Server(host="127.0.0.1", port=$PORT)
server.start()
EOF
)
