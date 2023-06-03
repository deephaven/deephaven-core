#!/bin/bash

# This script calls build-dependencies.sh to build all the C++ client dependencies, then downloads and builds the C++ client from source.
# The R client is entirely dependent on the C++ client, so this script must be run for a successful install from source.

export SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
export CLIENT=$SCRIPT_DIR/cpp-client

# First, we build all C++ dependencies
cd cpp-dependencies
chmod +x ./build-dependencies.sh
./build-dependencies.sh
cd $SCRIPT_DIR

# Next, we download, build, and install the Deephaven C++ client
svn checkout https://github.com/deephaven/deephaven-core/trunk/cpp-client
cd cpp-client/deephaven
mkdir build && cd build

export CLIENT=$SCRIPT_DIR/cpp-client
export DEEPHAVEN_LOCAL=$SCRIPT_DIR/cpp-dependencies/local
export CMAKE_PREFIX_PATH=${DEEPHAVEN_LOCAL}/abseil:${DEEPHAVEN_LOCAL}/boost:${DEEPHAVEN_LOCAL}/cares:${DEEPHAVEN_LOCAL}/flatbuffers:${DEEPHAVEN_LOCAL}/gflags:${DEEPHAVEN_LOCAL}/immer:${DEEPHAVEN_LOCAL}/protobuf:${DEEPHAVEN_LOCAL}/re2:${DEEPHAVEN_LOCAL}/zlib:${DEEPHAVEN_LOCAL}/grpc:${DEEPHAVEN_LOCAL}/arrow:${DEEPHAVEN_LOCAL}/deephaven
export NCPUS=$(getconf _NPROCESSORS_ONLN)
cmake -DCMAKE_INSTALL_PREFIX=${DEEPHAVEN_LOCAL}/deephaven .. && make -j$NCPUS install
