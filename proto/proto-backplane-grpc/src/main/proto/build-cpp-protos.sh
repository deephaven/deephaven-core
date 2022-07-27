#!/bin/bash

: ${DHDEPS_HOME:=$HOME/dhcpp}
: ${CPP_PROTO_BUILD_DIR:=build}

set -eux
mkdir -p "${CPP_PROTO_BUILD_DIR}"
$DHDEPS_HOME/local/protobuf/bin/protoc `find . -name \*.proto` --cpp_out=${CPP_PROTO_BUILD_DIR} --grpc_out=${CPP_PROTO_BUILD_DIR} --plugin=protoc-gen-grpc=$DHDEPS_HOME/local/grpc/bin/grpc_cpp_plugin
mv ${CPP_PROTO_BUILD_DIR}/deephaven/proto/*.{h,cc} ../../../../../cpp-client/deephaven/client/proto/deephaven/proto
