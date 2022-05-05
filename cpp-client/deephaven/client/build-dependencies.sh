#!/bin/bash

#
# Tested on Ubuntu 20.04
#

# Fail on first error; echo each command before executing.
set -eux


# Add anything to PATH that should take precendence here.
# export PATH=/l/cmake/3.21.2/bin:/l/gcc/11.2.0/bin:$PATH

# Edit to reflect your compiler preferences, or comment out for using system versions.
# export CC=/l/gcc/11.2.0/bin/gcc
# export CXX=/l/gcc/11.2.0/bin/g++

# Set to "Debug" or "Release"
: ${BUILD_TYPE:=Debug}

# Set to where you intend the sources for and installed depdenencies to live.
: ${DHDEPS_HOME:=$(pwd)}

# If you want to rebuild only certain parts or skip phases change "yes" to "no" below.
# Note this assumues you at least once built everything; otherwise dependencies among libraries
# may fail.
: ${CHECKOUT:=yes}
: ${BUILD_PROTOBUF:=yes}
: ${BUILD_RE2:=yes}
: ${BUILD_GFLAGS:=yes}
: ${BUILD_ABSL:=yes}
: ${BUILD_FLATBUFFERS:=yes}
: ${BUILD_CARES:=yes}
: ${BUILD_ZLIB:=yes}
: ${BUILD_GRPC:=yes}
: ${BUILD_ARROW:=yes}

#
# End of user customization section; you should not need to modify the code below
# unless you need to do partial re-builds.
#

# How many CPUs to use in -j arguments to make.
NCPUS=$(getconf _NPROCESSORS_ONLN)

# Where the checked out sources for dependencies will go
SRC=$DHDEPS_HOME/src

# Where the install prefix paths will go
PFX=$DHDEPS_HOME/local

# Let's get make to print out commands as they run
export VERBOSE=1

export CMAKE_PREFIX_PATH=${PFX}/abseil:${PFX}/cares:${PFX}/flatbuffers:${PFX}/gflags:${PFX}/protobuf:${PFX}/re2:${PFX}/zlib:${PFX}/grpc:${PFX}/arrow:${PFX}/deephaven

if [ ! -d $SRC ]; then
  mkdir -p $SRC
fi

if [ ! -d $PFX ]; then
  mkdir -p $PFX
fi

#
# Each phase below should explicitly change to expected current working directory before starting;
# there is no guarantee where the CWD is after a prior phase.
#

if [ "$CHECKOUT" = "yes" ]; then
  cd $SRC
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
  rm -f apache-arrow-5.0.0.tar.gz
fi 

### Protobuf
if [ "$BUILD_PROTOBUF" = "yes" ]; then
  echo
  echo "*** Building protobuf"
  cd $SRC/protobuf
  mkdir -p cmake/build && cd cmake/build
  cmake -Dprotobuf_BUILD_TESTS=OFF -DCMAKE_POSITION_INDEPENDENT_CODE=TRUE -DCMAKE_BUILD_TYPE=${BUILD_TYPE} -DCMAKE_INSTALL_PREFIX=${PFX}/protobuf ..
  make -j$NCPUS
  make install
fi

### re2
if [ "$BUILD_RE2" = "yes" ]; then
  echo
  echo "*** Building re2"
  cd $SRC/re2
  mkdir -p build && cd build
  cmake -DCMAKE_POSITION_INDEPENDENT_CODE=TRUE -DCMAKE_BUILD_TYPE=${BUILD_TYPE} -DCMAKE_INSTALL_PREFIX=${PFX}/re2 ..
  make -j$NCPUS
  make install
fi

### gflags
if [ "$BUILD_GFLAGS" = "yes" ]; then
  echo
  echo "*** Building gflags"
  cd $SRC/gflags
  mkdir -p build && cd build
  cmake -DCMAKE_BUILD_TYPE=${BUILD_TYPE} -DCMAKE_INSTALL_PREFIX=${PFX}/gflags ..
  make -j$NCPUS
  make install
fi

### absl
if [ "$BUILD_ABSL" = "yes" ]; then
  echo
  echo "*** Building abseil"
  cd $SRC/abseil-cpp
  mkdir -p cmake/build && cd cmake/build
  cmake -DCMAKE_POSITION_INDEPENDENT_CODE=TRUE -DCMAKE_BUILD_TYPE=${BUILD_TYPE} -DCMAKE_INSTALL_PREFIX=${PFX}/abseil  ../..
  make -j$NCPUS
  make install
fi

### flatbuffers
if [ "$BUILD_FLATBUFFERS" = "yes" ]; then
  echo
  echo "*** Building flatbuffers"
  cd $SRC/flatbuffers
  mkdir -p build && cd build
  cmake -DCMAKE_BUILD_TYPE=${BUILD_TYPE} -DCMAKE_INSTALL_PREFIX=${PFX}/flatbuffers ..
  make -j$NCPUS
  make install
fi

### c-ares
if [ "$BUILD_CARES" = "yes" ]; then
  echo
  echo "*** Building c-ares"
  cd $SRC/c-ares
  mkdir -p build && cd build
  cmake -DCMAKE_BUILD_TYPE=${BUILD_TYPE} -DCMAKE_INSTALL_PREFIX=${PFX}/cares ..
  make -j$NCPUS
  make install
fi

### zlib
if [ "$BUILD_ZLIB" = "yes" ]; then
  echo
  echo "*** Building zlib"
  cd $SRC/zlib
  mkdir -p build && cd build
  cmake -DCMAKE_BUILD_TYPE=${BUILD_TYPE} -DCMAKE_INSTALL_PREFIX=${PFX}/zlib ..
  make -j$NCPUS
  make install
fi

### grpc
if [ "$BUILD_GRPC" = "yes" ]; then
  echo
  echo "*** Building grpc"
  cd $SRC/grpc
  mkdir -p cmake/build && cd cmake/build
  cmake -DCMAKE_BUILD_TYPE=${BUILD_TYPE} -DCMAKE_POSITION_INDEPENDENT_CODE=TRUE -DCMAKE_INSTALL_PREFIX=${PFX}/grpc -DgRPC_INSTALL=ON \
        -DgRPC_ABSL_PROVIDER=package -DgRPC_CARES_PROVIDER=package -DgRPC_PROTOBUF_PROVIDER=package \
        -DgRPC_RE2_PROVIDER=package -DgRPC_SSL_PROVIDER=package -DgRPC_ZLIB_PROVIDER=package ../..
  make -j$NCPUS
  make install
fi

### arrow
if [ "$BUILD_ARROW" = "yes" ]; then
  echo
  echo "*** Building arrow"
  export CPATH=${PFX}/abseil/include${CPATH+:$CPATH}
  cd $SRC/apache-arrow-5.0.0/cpp
  mkdir -p build && cd build
  cmake -DARROW_BUILD_STATIC=ON -DARROW_FLIGHT=ON -DARROW_CSV=ON -DARROW_FILESYSTEM=ON -DARROW_DATASET=ON -DARROW_PARQUET=ON \
        -DARROW_WITH_BZ2=ON -DARROW_WITH_ZLIB=ON -DARROW_WITH_LZ4=ON -DARROW_WITH_SNAPPY=ON -DARROW_WITH_ZSTD=ON -DARROW_WITH_BROTLI=ON \
        -DCMAKE_BUILD_TYPE=${BUILD_TYPE} -DCMAKE_INSTALL_PREFIX=${PFX}/arrow ..
  make -j$NCPUS
  make install
fi

echo DONE.
