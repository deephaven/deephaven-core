#!/bin/bash

set -euxo pipefail

if [ -z ${PROTOC_BIN:+x} ] && [ -z ${DHCPP:+x} ]; then
    echo "$0: At least one of the environment variables 'PROTOC_BIN' and 'DHCPP' must be defined, aborting." 1>&2
    exit 1
fi

: ${PROTOC_BIN:=$DHCPP/bin}
: ${CPP_PROTO_BUILD_DIR:=build}

mkdir -p "${CPP_PROTO_BUILD_DIR}"
$DHCPP/bin/protoc \
    $(find . -name '*.proto') \
    --cpp_out=${CPP_PROTO_BUILD_DIR} \
    --grpc_out=${CPP_PROTO_BUILD_DIR} \
    --plugin=protoc-gen-grpc=${PROTOC_BIN}/grpc_cpp_plugin
mv -f ${CPP_PROTO_BUILD_DIR}/deephaven/proto/*.{h,cc} \
   ../../../../../cpp-client/deephaven/dhclient/proto/deephaven/proto
