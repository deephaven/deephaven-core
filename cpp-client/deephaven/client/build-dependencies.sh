#!/bin/bash

#
# Tested on Ubuntu 20.04
#

set -eux

NCPUS=$(getconf _NPROCESSORS_ONLN)
BUILD_TYPE=Debug

export MYSRC=$HOME/dhcpp/src
export PFX=$HOME/dhcpp/lib
export VERBOSE=1  # Let's get make to print out commands as they run
export CMAKE_PREFIX_PATH=${PFX}/abseil:${PFX}/cares:${PFX}/flatbuffers:${PFX}/gflags:${PFX}/protobuf:${PFX}/re2:${PFX}/zlib:${PFX}/grpc:${PFX}/arrow:${PFX}/deephaven


cd $MYSRC
git clone -b v3.18.0 --depth 1 https://github.com/protocolbuffers/protobuf.git
git clone -b 2021-09-01 --depth 1 https://github.com/google/re2.git
git clone -b v2.2.2 --depth 1 https://github.com/gflags/gflags.git
git clone -b 20210324.2 --depth 1 https://github.com/abseil/abseil-cpp.git
git clone -b v2.0.0 --depth 1 https://github.com/google/flatbuffers.git
git clone -b cares-1_17_2 --depth 1 https://github.com/c-ares/c-ares.git
git clone -b v1.2.11 --depth 1 https://github.com/madler/zlib
git clone -b v1.38.0 --depth 1 https://github.com/grpc/grpc
wget 'https://www.apache.org/dyn/closer.lua?action=download&filename=arrow/arrow-5.0.0/apache-arrow-5.0.0.tar.gz' -O apache-arrow-5.0.0.tar.gz
tar xfz apache-arrow-5.0.0.tar.gz
mkdir -p ${PFX}

### Protobuf
echo
echo "*** Building protobuf"
cd $MYSRC/protobuf
mkdir -p cmake/build && cd cmake/build
cmake -Dprotobuf_BUILD_TESTS=OFF -DCMAKE_POSITION_INDEPENDENT_CODE=TRUE -DCMAKE_BUILD_TYPE=${BUILD_TYPE} -DCMAKE_INSTALL_PREFIX=${PFX}/protobuf ..
make -j$NCPUS
make install

### re2
echo
echo "*** Building re2"
cd $MYSRC/re2
mkdir build && cd build
cmake -DCMAKE_POSITION_INDEPENDENT_CODE=TRUE -DCMAKE_BUILD_TYPE=${BUILD_TYPE} -DCMAKE_INSTALL_PREFIX=${PFX}/re2 ..
make -j$NCPUS
make install
cd ../..

### gflags
echo
echo "*** Building gflags"
cd $MYSRC/gflags
mkdir build && cd build
cmake -DCMAKE_BUILD_TYPE=${BUILD_TYPE} -DCMAKE_INSTALL_PREFIX=${PFX}/gflags ..
make -j$NCPUS
make install

### absl
echo
echo "*** Building abseil"
cd $MYSRC/abseil-cpp
mkdir -p cmake/build && cd cmake/build
cmake -DCMAKE_POSITION_INDEPENDENT_CODE=TRUE -DCMAKE_BUILD_TYPE=${BUILD_TYPE} -DCMAKE_INSTALL_PREFIX=${PFX}/abseil  ../..
make -j$NCPUS
make install
cd ../..

### flatbuffers
echo
echo "*** Building flatbuffers"
cd $MYSRC/flatbuffers
mkdir build && cd build
cmake -DCMAKE_BUILD_TYPE=${BUILD_TYPE} -DCMAKE_INSTALL_PREFIX=${PFX}/flatbuffers ..
make -j$NCPUS
make install

echo
echo "*** Building c-ares"
### c-ares
cd $MYSRC/c-ares
mkdir build && cd build
cmake -DCMAKE_BUILD_TYPE=${BUILD_TYPE} -DCMAKE_INSTALL_PREFIX=${PFX}/cares ..
make -j$NCPUS
make install

### zlib
echo
echo "*** Building zlib"
cd $MYSRC/zlib
mkdir build && cd build
cmake -DCMAKE_BUILD_TYPE=${BUILD_TYPE} -DCMAKE_INSTALL_PREFIX=${PFX}/zlib ..
make -j$NCPUS
make install

### grpc
echo
echo "*** Building grpc"
cd $MYSRC/grpc
mkdir -p cmake/build && cd cmake/build
cmake -DCMAKE_BUILD_TYPE=${BUILD_TYPE} -DCMAKE_POSITION_INDEPENDENT_CODE=TRUE -DCMAKE_INSTALL_PREFIX=${PFX}/grpc -DgRPC_INSTALL=ON \
      -DgRPC_ABSL_PROVIDER=package -DgRPC_CARES_PROVIDER=package -DgRPC_PROTOBUF_PROVIDER=package \
      -DgRPC_RE2_PROVIDER=package -DgRPC_SSL_PROVIDER=package -DgRPC_ZLIB_PROVIDER=package ../..
make -j$NCPUS
make install

### arrow
echo
echo "*** Building arrow"
export CPATH=${PFX}/abseil/include${CPATH+:$CPATH}
cd $MYSRC/apache-arrow-5.0.0/cpp
mkdir build && cd build
cmake -DARROW_BUILD_STATIC=ON -DARROW_FLIGHT=ON -DARROW_CSV=ON -DARROW_FILESYSTEM=ON -DARROW_DATASET=ON -DARROW_PARQUET=ON \
      -DARROW_WITH_BZ2=ON -DARROW_WITH_ZLIB=ON -DARROW_WITH_LZ4=ON -DARROW_WITH_SNAPPY=ON -DARROW_WITH_ZSTD=ON -DARROW_WITH_BROTLI=ON \
      -DCMAKE_BUILD_TYPE=${BUILD_TYPE} -DCMAKE_INSTALL_PREFIX=${PFX}/arrow ..
make -j$NCPUS
make install
