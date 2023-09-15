#!/bin/bash

set -euxo pipefail

: ${DHCPP:=$HOME/dhcpp}
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
