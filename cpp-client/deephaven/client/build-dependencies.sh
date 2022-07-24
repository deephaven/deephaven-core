#!/bin/bash

#
# Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
#

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
: ${BUILD_FLATBUFFERS:=no}
: ${BUILD_CARES:=yes}
: ${BUILD_ZLIB:=yes}
: ${BUILD_GRPC:=yes}
: ${BUILD_ARROW:=yes}
: ${BUILD_IMMER:=yes}
: ${BUILD_BOOST:=yes}

: ${BOOST_VERSION:=1_79_0}

# At the point of this writing, the latest immer release is pretty old.
# We want something a lot more recent, but don't want to track head as is a moving
# target and we can't guarantee things will continue to compile/be consistent.
# So we select a particular SHA.
: ${IMMER_SHA:=e5d79ed80ec74d511cc4f52fb68feeac66507f2c}

#
# End of user customization section; you should not need to modify the code below
# unless you need to do partial re-builds.
#

# How many CPUs to use in -j arguments to make.
: ${NCPUS:=$(getconf _NPROCESSORS_ONLN)}

# Where the checked out sources for dependencies will go
: ${SRC:=$DHDEPS_HOME/src}

# Where the install prefix paths will go
: ${PFX:=$DHDEPS_HOME/local}

# Let's get make to print out commands as they run
export VERBOSE=1

export CMAKE_PREFIX_PATH=\
${PFX}/abseil:\
${PFX}/cares:\
${PFX}/flatbuffers:\
${PFX}/gflags:\
${PFX}/protobuf:\
${PFX}/re2:\
${PFX}/zlib:\
${PFX}/grpc:\
${PFX}/arrow:\
${PFX}/boost:\
${PFX}/immer:\
${PFX}/deephaven

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

: ${GIT_FLAGS:="--quiet -c advice.detachedHead=false"}

if [ "$CHECKOUT" = "yes" ]; then
  cd $SRC
  git clone $GIT_FLAGS -b v3.20.1 --depth 1 https://github.com/protocolbuffers/protobuf.git
  git clone $GIT_FLAGS -b 2022-04-01 --depth 1 https://github.com/google/re2.git
  git clone $GIT_FLAGS -b v2.2.2 --depth 1 https://github.com/gflags/gflags.git
  git clone $GIT_FLAGS -b 20210324.2 --depth 1 https://github.com/abseil/abseil-cpp.git
  git clone $GIT_FLAGS -b v2.0.6 --depth 1 https://github.com/google/flatbuffers.git
  git clone $GIT_FLAGS -b cares-1_18_1 --depth 1 https://github.com/c-ares/c-ares.git
  git clone $GIT_FLAGS -b v1.2.11 --depth 1 https://github.com/madler/zlib
  git clone $GIT_FLAGS -b v1.45.2 --depth 1 https://github.com/grpc/grpc
  git clone $GIT_FLAGS -b apache-arrow-7.0.0 --depth 1 https://github.com/apache/arrow
  git clone $GIT_FLAGS https://github.com/arximboldi/immer.git && (cd immer && git checkout "${IMMER_SHA}")
  curl -sL https://boostorg.jfrog.io/artifactory/main/release/1.79.0/source/boost_"${BOOST_VERSION}".tar.bz2 | tar jxf -
  # Apply apache arrow patch.
  (cd arrow && patch -p1 <<EOF
diff --git a/cpp/src/arrow/ipc/reader.cc b/cpp/src/arrow/ipc/reader.cc
index 0b46203..6fe1308 100644
--- a/cpp/src/arrow/ipc/reader.cc
+++ b/cpp/src/arrow/ipc/reader.cc
@@ -528,7 +528,7 @@ Result<std::shared_ptr<RecordBatch>> LoadRecordBatchSubset(
       auto column = std::make_shared<ArrayData>();
       RETURN_NOT_OK(loader.Load(&field, column.get()));
       if (metadata->length() != column->length) {
-        return Status::IOError("Array length did not match record batch length");
+        // return Status::IOError("Array length did not match record batch length");
       }
       columns[i] = std::move(column);
       if (inclusion_mask) {
EOF
)
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
  cmake -DCMAKE_CXX_STANDARD=11 -DCMAKE_POSITION_INDEPENDENT_CODE=TRUE -DCMAKE_BUILD_TYPE=${BUILD_TYPE} -DCMAKE_INSTALL_PREFIX=${PFX}/abseil  ../..
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
  export CPATH=${PFX}/protobuf/include${CPATH+:$CPATH}
  cd $SRC/arrow/cpp
  mkdir -p build && cd build
  cmake -DARROW_BUILD_STATIC=ON -DARROW_FLIGHT=ON -DARROW_CSV=ON -DARROW_FILESYSTEM=ON -DARROW_DATASET=ON -DARROW_PARQUET=ON \
        -DARROW_WITH_BZ2=ON -DARROW_WITH_ZLIB=ON -DARROW_WITH_LZ4=ON -DARROW_WITH_SNAPPY=ON -DARROW_WITH_ZSTD=ON -DARROW_WITH_BROTLI=ON \
	-DARROW_SIMD_LEVEL=NONE \
        -DCMAKE_BUILD_TYPE=${BUILD_TYPE} -DCMAKE_INSTALL_PREFIX=${PFX}/arrow ..
  make -j$NCPUS
  make install
fi

### immer
if [ "$BUILD_IMMER" = "yes" ]; then
  echo
  echo "*** Building immer"
  cd $SRC/immer
  mkdir -p build && cd build
  cmake -DCMAKE_BUILD_TYPE=${BUILD_TYPE} -DCMAKE_INSTALL_PREFIX=${PFX}/immer ..
  make -j$NCPUS
  make install
fi

### boost
if [ "$BUILD_BOOST" = "yes" ]; then
  echo
  echo "*** Building boost"
  cd $SRC/boost_"${BOOST_VERSION}"
  ./bootstrap.sh --prefix=${PFX}/boost
  ./b2 install
fi

echo DONE.
echo "Use CMAKE_PREFIX_PATH=$CMAKE_PREFIX_PATH"
