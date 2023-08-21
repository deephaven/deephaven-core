#!/bin/bash

set -euxo pipefail

: ${DHCPP_LOCAL:=$HOME/dhcpp/local}
: ${CPP_PROTO_BUILD_DIR:=build}

mkdir -p "${CPP_PROTO_BUILD_DIR}"
$DHCPP_LOCAL/protobuf/bin/protoc \
    $(find . -name '*.proto') \
    --cpp_out=${CPP_PROTO_BUILD_DIR} \
    --grpc_out=${CPP_PROTO_BUILD_DIR} \
    --plugin=protoc-gen-grpc=$DHCPP_LOCAL/bin/grpc_cpp_plugin
mv -f ${CPP_PROTO_BUILD_DIR}/deephaven/proto/*.{h,cc} \
   ../../../../../cpp-client/deephaven/dhclient/proto/deephaven/proto
