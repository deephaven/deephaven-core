#!/bin/bash

#
# Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
#

#
# Tested on Ubuntu 22.04
#
# On Ubuntu, the following packages need to be installed:
#
# $ sudo apt -y install git g++ cmake make build-essential zlib1g-dev libssl-dev pkg-config
#
# On Fedora, the following packages need to be installed:
#
# $ sudo dnf -y groupinstall 'Development Tools'
# $ sudo dnf -y install cmake gcc-c++ openssl-devel
#

# Fail on first error; echo each command before executing.
set -euo pipefail

if [ -f /etc/redhat-release ]; then
    fedora=yes
    debian=no
    if grep -q "^Fedora release 38 " /etc/redhat-release; then
      fedora38=yes
    else
      fedora38=no
    fi
elif [ -f /etc/issue ] && grep -qE 'Ubuntu|Debian' /etc/issue; then
    debian=yes
    fedora=no
    fedora38=no
else
  echo "$0: Unsupported platform: not fedora, not ubuntu, aborting." 1>&2
  exit 1
fi

function usage {
    echo "Usage: $0 [--help|-h] [--clean] [--shared] [--static-pic] [--multilocal] [action]"
    echo "   where action is one or more of"
    echo "     *  {protobuf|re2|gflags|abseil|flatbuffers|cares|zlib|grpc|arrow|immer}"
    echo "     *  any of the above prefixed by clone- or build-"
    echo "     *  'env'"
    echo "   For example:"
    echo "    protobuf clone-re2 build-gflags env"
    echo "  means \"clone and build protobuf, clone re2 (but don't build), build gflags (without cloning),"
    echo "  and create the env file.\""
    echo
    echo "  Options:"
    echo "    --clean          Remove the downloaded sources once a library is built."
    echo "                     This is necessary for re-cloning."
    echo "    --shared         Build shared libraries.  This is the default if not specified."
    echo "    --static-pic     Build static libraries from object files compiled with -fPIC"
    echo "                     (position independent code).  This is useful for creating"
    echo "                     self-contained bundled libraries that contain all the dependencies"
    echo "                     (eg, deephaven.so).  This can be used to produce python and R clients"
    echo "                     that do not depend on the dynamic libraries somewhere else in the filesystem."
    echo "                     In the past this was the default (now the default is --shared)."
    echo "    --multilocal     Install each library in its own subdirectory under 'local'.  This used to be the default."
    echo "    --help|-h        Show this usage message and exit."
    echo
    echo "  Environment variables:"
    echo "    GITHUB_BASE_URL  If defined, this will replace the default of 'https://github.com' for git operations."
    echo
    echo "  If no actions are requested, this results in performing all default actions."
    echo "  When no individual actions are requested, an environment variable of similar"
    echo "  name to the corresponding option (eg, CLONE_PROTOBUF) can be defined"
    echo "  and assigned to either 'yes' or 'no' to change the list of default actions"
    echo "  Default actions:"
    echo "    CLONE_PROTOBUF=yes"
    echo "    BUILD_PROTOBUF=yes"
    echo "    CLONE_RE2=yes"
    echo "    BUILD_RE2=yes"
    echo "    CLONE_GFLAGS=yes"
    echo "    BUILD_GFLAGS=yes"
    echo "    CLONE_ABSEIL=yes"
    echo "    BUILD_ABSEIL=yes"
    echo "    CLONE_FLATBUFFERS=no"
    echo "    BUILD_FLATBUFFERS=no"
    echo "    CLONE_CARES=yes"
    echo "    BUILD_CARES=yes"
    echo "    CLONE_ZLIB=yes"
    echo "    BUILD_ZLIB=yes"
    echo "    CLONE_GRPC=yes"
    echo "    BUILD_GRPC=yes"
    echo "    CLONE_ARROW=yes"
    echo "    BUILD_ARROW=yes"
    echo "    CLONE_IMMER=yes"
    echo "    BUILD_IMMER=yes"
    echo "    GENERATE_ENV=yes"
    echo
    echo "  For the case where options are provided, each option of the form"
    echo "  clone-\${libraryname} and build-\${libraryname} respectively"
    echo "  clones the dependent library source and builds it."
    echo "  The env option generates a shell source file called"
    echo "  env.sh that contains the cmake variable definitions needed to use"
    echo "  the dependent libraries from the locations they are being built"
    echo "  by this script."
    echo
    echo "  Note that flatbuffers is special, is not normally required,"
    echo "  and when updating it it requires additional manual steps."
    echo "  See comments in this script for details about updating flatbuffers."
    echo
    echo "  Examples:"
    echo "    * to clone and build all dependencies, do not set any"
    echo "      related environment variables, and just call: $0"
    echo "    * to build an already cloned re2 and clone and build abseil,"
    echo "      call: $0 build-re2 clone-abseil build-abseil"
    echo "    * to avoid cloning protobuf, but otherwise do all other"
    echo "      default actions (this assumes protoqbuf was cloned earlier),"
    echo "      call: CLONE_PROTOBUF=no $0"
    echo
    echo "  This script produces a lot of output.  It is recommended you save"
    echo "  the output to a file in case you need to see details of"
    echo "  actions performed later; eg, from bash you can run:"
    echo
    echo "    $0 2>&1 | tee $0.log"
}

function prefix {
  if [ "$#" -ne 1 ]; then
    echo "$0: Internal error: prefix called with $# arguments." 1>&2
    exit 1
  fi
  if [ "$multilocal" = "yes" ]; then
    echo "$PFX/local/$1"
  else
    echo "$PFX"
  fi
}

clean="no"
multilocal="no"

shared="yes"
cmake_shared_arg="-DBUILD_SHARED_LIBS=ON"
cmake_pic_arg=""

shared_opt=""

while [ "$#" -ge 1 ]; do
    if [ "$1" == "--clean" ]; then
        clean="yes"
        shift
        continue
    fi
    if [ "$1" == "--shared" ]; then
        if [ "$shared_opt" != "" ]; then
            echo "$0: can't use both --shared and $shared_opt." 1>&2
            exit 1
        fi
        shared_opt="--shared"
        # Already configured as default
        shift
        continue
    fi
    if [ "$1" == "--static-pic" ]; then
        if [ "$shared_opt" != "" ]; then
            echo "$0: can't use both --static-pic and $shared_opt." 1>&2
            exit 1
        fi
        shared_opt="--static-pic"
        shared=no
        cmake_shared_arg=""
        cmake_pic_arg="-DCMAKE_POSITION_INDEPENDENT_CODE=TRUE"
        shared_explcitly_selected="yes"
        shift
        continue
    fi
    if [ "$1" == "--static" ]; then
        if [ "$shared_opt" != "" ]; then
            echo "$0: can't use both --static and $shared_opt." 1>&2
            exit 1
        fi
        shared_opt="--static"
        shared=no
        cmake_shared_arg=""
        cmake_pic_arg=""
        shared_explcitly_selected="yes"
        shift
        continue
    fi
    if [ "$1" == "--multilocal" ]; then
        multilocal="yes"
        shift
        continue
    fi
    if [ "$1" == "--help" ] || [ "$1" == "-h" ]; then
        usage
        exit 0
    fi
    break
done

: ${GITHUB_BASE_URL:="https://github.com"}

if [ "$#" -eq 0 ]; then
    : ${CLONE_PROTOBUF:=yes}
    : ${BUILD_PROTOBUF:=yes}
    : ${CLONE_RE2:=yes}
    : ${BUILD_RE2:=yes}
    : ${CLONE_GFLAGS:=yes}
    : ${BUILD_GFLAGS:=yes}
    : ${CLONE_ABSEIL:=yes}
    : ${BUILD_ABSEIL:=yes}
    : ${CLONE_FLATBUFFERS:=no}
    : ${BUILD_FLATBUFFERS:=no}
    : ${CLONE_CARES:=yes}
    : ${BUILD_CARES:=yes}
    : ${CLONE_ZLIB:=yes}
    : ${BUILD_ZLIB:=yes}
    : ${CLONE_GRPC:=yes}
    : ${BUILD_GRPC:=yes}
    : ${CLONE_ARROW:=yes}
    : ${BUILD_ARROW:=yes}
    : ${CLONE_IMMER:=yes}
    : ${BUILD_IMMER:=yes}
    : ${GENERATE_ENV:=yes}
else
    CLONE_PROTOBUF=no
    BUILD_PROTOBUF=no
    CLONE_RE2=no
    BUILD_RE2=no
    CLONE_GFLAGS=no
    BUILD_GFLAGS=no
    CLONE_ABSEIL=no
    BUILD_ABSEIL=no
    CLONE_FLATBUFFERS=no
    BUILD_FLATBUFFERS=no
    CLONE_CARES=no
    BUILD_CARES=no
    CLONE_ZLIB=no
    BUILD_ZLIB=no
    CLONE_GRPC=no
    BUILD_GRPC=no
    CLONE_ARROW=no
    BUILD_ARROW=no
    CLONE_IMMER=no
    BUILD_IMMER=no
    GENERATE_ENV=no
    while [ "$#" -gt 0 ]; do
        case "$1" in
            clone-protobuf)
                CLONE_PROTOBUF=yes
                shift
                ;;
            build-protobuf)
                BUILD_PROTOBUF=yes
                shift
                ;;
            protobuf)
                CLONE_PROTOBUF=yes
                BUILD_PROTOBUF=yes
                shift
                ;;
            clone-re2)
                CLONE_RE2=yes
                shift
                ;;
            build-re2)
                BUILD_RE2=yes
                shift
                ;;
            re2)
                CLONE_RE2=yes
                BUILD_RE2=yes
                shift
                ;;
            clone-gflags)
                CLONE_GFLAGS=yes
                shift
                ;;
            build-gflags)
                BUILD_GFLAGS=yes
                shift
                ;;
            gflags)
                CLONE_GFLAGS=yes
                BUILD_GFLAGS=yes
                shift
                ;;
            clone-abseil)
                CLONE_ABSEIL=yes
                shift
                ;;
            build-abseil)
                BUILD_ABSEIL=yes
                shift
                ;;
            abseil)
                CLONE_ABSEIL=yes
                BUILD_ABSEIL=yes
                shift
                ;;
            clone-flatbuffers)
                CLONE_FLATBUFFERS=yes
                shift
                ;;
            build-flatbuffers)
                BUILD_FLATBUFFERS=yes
                shift
                ;;
            flatbuffers)
                CLONE_FLATBUFFERS=yes
                BUILD_FLATBUFFERS=yes
                shift
                ;;
            clone-cares)
                CLONE_CARES=yes
                shift
                ;;
            build-cares)
                BUILD_CARES=yes
                shift
                ;;
            cares)
                CLONE_CARES=yes
                BUILD_CARES=yes
                shift
                ;;
            clone-zlib)
                CLONE_ZLIB=yes
                shift
                ;;
            build-zlib)
                BUILD_ZLIB=yes
                shift
                ;;
            zlib)
                CLONE_ZLIB=yes
                BUILD_ZLIB=yes
                shift
                ;;
            clone-grpc)
                CLONE_GRPC=yes
                shift
                ;;
            build-grpc)
                BUILD_GRPC=yes
                shift
                ;;
            grpc)
                CLONE_GRPC=yes
                BUILD_GRPC=yes
                shift
                ;;
            clone-arrow)
                CLONE_ARROW=yes
                shift
                ;;
            build-arrow)
                BUILD_ARROW=yes
                shift
                ;;
            arrow)
                CLONE_ARROW=yes
                BUILD_ARROW=yes
                shift
                ;;
            clone-immer)
                CLONE_IMMER=yes
                shift
                ;;
            build-immer)
                BUILD_IMMER=yes
                shift
                ;;
            immer)
                CLONE_IMMER=yes
                BUILD_IMMER=yes
                shift
                ;;
            env)
                GENERATE_ENV=yes
                shift
                ;;
            *)
                echo "$0: unrecognized option: '$1'" 1>&2
                usage 1>&2
                exit 1
                ;;
        esac
    done
fi

# Enable trace output for the actual work,
# which may help debug issues and makes more explicit in the output
# what the script is doing.
set -x

# Add anything to PATH that should take precendence here.
# export PATH=/l/cmake/3.21.2/bin:/l/gcc/11.2.0/bin:$PATH

# Edit to reflect your compiler preferences, or comment out for using system versions.
# export CC=/l/gcc/11.2.0/bin/gcc
# export CXX=/l/gcc/11.2.0/bin/g++

# Set to "Debug", "Release", or "RelWithDebInfo".
: ${BUILD_TYPE:=RelWithDebInfo}

# Set to where you intend the sources for and installed depdenencies to live.
: ${DHDEPS_HOME:=$(pwd)}

#
# End of user customization section; you should not need to modify the code below
# unless you need to do partial re-builds.
#

# How many CPUs to use in -j arguments to make.
: ${NCPUS:=$(getconf _NPROCESSORS_ONLN)}

# Where the checked out sources for dependencies will go
: ${SRC:=$DHDEPS_HOME/src}

# Where the install prefix paths will go
: ${PFX:=$DHDEPS_HOME}

# Let's get make to print out commands as they run
export VERBOSE=1

all_libs="
PROTOBUF
RE2
GFLAGS
ABSEIL
FLATBUFFERS
CARES
ZLIB
GRPC
ARROW
IMMER
"

if [ "$multilocal" = "yes" ]; then
  CMAKE_PREFIX_PATH=""
  for lib in $all_libs; do
    build_var="BUILD_${lib}"
    if [ "${!build_var}" = "yes" ]; then
      lib_dir=$(echo $lib | tr '[A-Z]' '[a-z]')
      CMAKE_PREFIX_PATH+=":${PFX}/${lib_dir}"
    fi
  done
  CMAKE_PREFIX_PATH+=":${PFX}/deephaven"
else
  CMAKE_PREFIX_PATH="${PFX}"
fi
export CMAKE_PREFIX_PATH

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

BUILD_DIR=build_dir

cmake_common_args="${cmake_pic_arg} ${cmake_shared_arg} -DCMAKE_BUILD_TYPE=${BUILD_TYPE}"
# In some platforms (eg, fedora) some libraries (eg, arrow, abseil) end up installing
# libraries in `lib64` instead of `lib`.  We want to keep our LD_LIBRARY_PATH simple.
cmake_common_args+=" -DCMAKE_INSTALL_LIBDIR=lib"

if [ "$shared" = "yes" ]; then
  if [ "$multilocal" = "yes" ]; then
    LD_LIBRARY_PATH=""
    for lib in $all_libs; do
      build_var="BUILD_${lib}"
      if [ "${!build_var}" = "yes" ]; then
        lib_dir=$(echo $lib | tr '[A-Z]' '[a-z]')
        LD_LIBRARY_PATH+=":${PFX}/${lib_dir}/lib"
      fi
    done
  else
    LD_LIBRARY_PATH="${PFX}/lib"
  fi
  export LD_LIBRARY_PATH
fi

### abseil
if [ "$CLONE_ABSEIL" = "yes" ]; then
  echo
  echo "*** Clone abseil"
  cd $SRC
  # Previously used version: 20211102.0
  git clone $GIT_FLAGS -b 20240116.0 --depth 1 "${GITHUB_BASE_URL}/abseil/abseil-cpp.git"
  echo "*** Cloning abseil DONE"
  if [ "$fedora38" = "yes" ]; then
  echo "*** Patching abseil for Fedora 38"
    patch -p0 <<'END'
--- abseil-cpp/absl/strings/internal/str_format/extension.h.orig        2023-09-21 03:15:05.004224385 +0000
+++ abseil-cpp/absl/strings/internal/str_format/extension.h     2023-09-21 03:15:23.408208301 +0000
@@ -19,6 +19,7 @@
 #include <limits.h>

 #include <cstddef>
+#include <cstdint>
 #include <cstring>
END
  fi
fi
if [ "$BUILD_ABSEIL" = "yes" ]; then
  echo
  echo "*** Building abseil"
  cd $SRC/abseil-cpp
  mkdir -p "cmake/$BUILD_DIR" && cd "cmake/$BUILD_DIR"
  cmake -DCMAKE_CXX_STANDARD=17 \
	-DABSL_PROPAGATE_CXX_STD=ON \
        ${cmake_common_args} \
        -DCMAKE_INSTALL_PREFIX=$(prefix abseil) \
        ../..
  make -j$NCPUS
  make install
  cd ../.. && rm -fr "cmake/$BUILD_DIR"
  if [ "$clean" = "yes" ]; then
    rm -fr "$SRC/abseil-cpp"
  fi
  echo "*** Building abseil DONE"
fi

### zlib
if [ "$CLONE_ZLIB" = "yes" ]; then
  echo
  echo "*** Clone zlib"
  cd $SRC
  # Previously used version: v1.3
  git clone $GIT_FLAGS -b v1.3.1 --depth 1 "${GITHUB_BASE_URL}/madler/zlib"
  echo "*** Cloning zlib DONE"
fi
if [ "$BUILD_ZLIB" = "yes" ]; then
  echo
  echo "*** Building zlib"
  cd $SRC/zlib
  mkdir -p "$BUILD_DIR" && cd "$BUILD_DIR"
  cmake ${cmake_common_args} \
        -DCMAKE_INSTALL_PREFIX=$(prefix zlib) \
        ..
  make -j$NCPUS
  make install
  cd .. && rm -fr "$BUILD_DIR"
  if [ "$shared" != "yes" ]; then
    # We want to avoid anything linking against shared libraries,
    # and there is no way to ask zlib build to not generate shared libraries...
    rm -f ${PFX}/zlib/lib/libz.so*
  fi
  if [ "$clean" = "yes" ]; then
    rm -fr "$SRC/zlib"
  fi
  echo "*** Building zlib DONE"
fi

### Protobuf
if [ "$CLONE_PROTOBUF" = "yes" ]; then
  echo
  echo "*** Cloning protobuf"
  cd $SRC
  # Previously used version: v25.3
  git clone $GIT_FLAGS -b v28.1 --depth 1 "${GITHUB_BASE_URL}/protocolbuffers/protobuf.git"
  echo "*** Cloning protobuf DONE"
fi
if [ "$BUILD_PROTOBUF" = "yes" ]; then
  echo
  echo "*** Building protobuf"
  cd $SRC/protobuf
  # Note in the previously used version we built inside cmake/build; not anymore.
  mkdir -p "$BUILD_DIR" && cd "$BUILD_DIR"
  cmake -Dprotobuf_BUILD_TESTS=OFF \
        ${cmake_common_args} \
        -DCMAKE_BUILD_TYPE=${BUILD_TYPE} \
        -Dprotobuf_ABSL_PROVIDER=package \
        -DCMAKE_INSTALL_PREFIX=$(prefix protobuf) \
        ..
  make -j$NCPUS
  make install
  cd .. && rm -fr "$BUILD_DIR"
  if [ "$clean" = "yes" ]; then
    rm -fr "$SRC/protobuf"
  fi
  echo "*** Building protobuf DONE"
fi

### re2
if [ "$CLONE_RE2" = "yes" ]; then
  echo
  echo "*** Cloning re2"
  cd $SRC
  # Previously used version: 2022-06-01
  git clone $GIT_FLAGS -b 2024-07-02 --depth 1 "${GITHUB_BASE_URL}/google/re2.git"
  echo "*** Cloning re2 DONE"
fi
if [ "$BUILD_RE2" = "yes" ]; then
  echo
  echo "*** Building re2"
  cd $SRC/re2
  mkdir -p "$BUILD_DIR" && cd "$BUILD_DIR"
  cmake ${cmake_common_args} \
        -DCMAKE_INSTALL_PREFIX=$(prefix re2) \
        ..
  make -j$NCPUS
  make install
  cd .. && rm -fr "$BUILD_DIR"
  if [ "$clean" = "yes" ]; then
    rm -fr "$SRC/re2"
  fi
  echo "*** Building re2 DONE"
fi

### gflags
if [ "$CLONE_GFLAGS" = "yes" ]; then
  echo
  echo "*** Cloning gflags"
  cd $SRC
  git clone $GIT_FLAGS -b v2.2.2 --depth 1 "${GITHUB_BASE_URL}/gflags/gflags.git"
  echo "*** Cloning gflags DONE"
fi
if [ "$BUILD_GFLAGS" = "yes" ]; then
  echo
  echo "*** Building gflags"
  cd $SRC/gflags
  mkdir -p "$BUILD_DIR" && cd "$BUILD_DIR"
  cmake ${cmake_common_args} \
        -DCMAKE_INSTALL_PREFIX=$(prefix gflags) \
        ..
  make -j$NCPUS
  make install
  cd .. && rm -fr "$BUILD_DIR"
  if [ "$clean" = "yes" ]; then
    rm -fr "$SRC/gflags"
  fi
  echo "*** Building gflags DONE"
fi

### flatbuffers
# In typical situations you don't need the flatbuffers library, because Deephaven
# has vendored the flatbuffers library (currently v2.0.6), taken the subset of files
# needed, and changed its namespace to avoid any chance of conflict with other
# instances of flatbuffers (specifically, the one linked by Apache Arrow).
#
# The only scenario where you may need to build flatbuffers v2.0.6 is if the Barrage
# format changes (https://github.com/deephaven/barrage/blob/main/format/Barrage.fbs)
# and you want to re-run flatc.
#
# On the other hand if you decide to move Deephaven to a newer version of flatbuffers,
# you should change this script to check out and build that
# newer version, copy over all the modified files to the appropriate place off of
# $DHSRC/cpp-client/deephaven/dhcore/third_party/flatbuffers, fix their namespaces, and
# update the patch.001 and README.md files. You will also need to rerun flatc with
# the newly compiled library.
#
# To rerun flatc, you need to run the following steps
# cd $BARRAGE_REPO/format
# echo "root_type BarrageMessageWrapper;" >> Barrage.fbs  # TODO make this change permanent
# $DHCPP/lib/flatbuffers/bin/flatc --cpp Barrage.fbs
# mv Barrage_generated. $DHSRC/cpp-client/deephaven/dhcore/flatbuf/deephaven/flatbuf
if [ "$CLONE_FLATBUFFERS" = "yes" ]; then
  echo
  echo "*** Clone flatbuffers"
  cd $SRC
  # Previously used version: v2.0.6
  git clone $GIT_FLAGS -b v23.5.26 --depth 1 "${GITHUB_BASE_URL}/google/flatbuffers.git"
  echo "*** Cloning flatbuffers DONE"
fi
if [ "$BUILD_FLATBUFFERS" = "yes" ]; then
  echo
  echo "*** Building flatbuffers"
  cd $SRC/flatbuffers
  mkdir -p "$BUILD_DIR" && cd "$BUILD_DIR"
  cmake ${cmake_common_args} \
        -DCMAKE_INSTALL_PREFIX=$(prefix flatbuffers) \
        ..
  make -j$NCPUS
  make install
  cd .. && rm -fr "$BUILD_DIR"
  if [ "$clean" = "yes" ]; then
    rm -fr "$SRC/flatbuffers"
  fi
  echo "*** Building flatbuffers DONE"
fi

### c-ares
if [ "$CLONE_CARES" = "yes" ]; then
  echo
  echo "*** Clone ares"
  cd $SRC
  # Previously used version: cares-1_28_1
  git clone $GIT_FLAGS -b v1.33.1 --depth 1 "${GITHUB_BASE_URL}/c-ares/c-ares.git"
  echo "*** Cloning ares DONE"
fi
if [ "$BUILD_CARES" = "yes" ]; then
  if [ "$shared" = "yes" ]; then
    cmake_cares_extra_args="-DCARES_SHARED=ON -DCARES_STATIC=ON"
  else
    cmake_cares_extra_args="-DCARES_SHARED=OFF -DCARES_STATIC=ON"
  fi
  echo
  echo "*** Building c-ares"
  cd $SRC/c-ares
  mkdir -p "$BUILD_DIR" && cd "$BUILD_DIR"
  cmake ${cmake_common_args} \
        ${cmake_cares_extra_args} \
        -DCMAKE_INSTALL_PREFIX=$(prefix cares) \
        ..
  make -j$NCPUS
  make install
  cd .. && rm -fr "$BUILD_DIR"
  if [ "$clean" = "yes" ]; then
    rm -fr "$SRC/c-ares"
  fi
  echo "*** Building ares DONE"
  unset cmake_cares_extra_args
fi

### grpc
if [ "$CLONE_GRPC" = "yes" ]; then
  echo
  echo "*** Clone grpc"
  cd $SRC
  # Previously used version: v1.63.0
  git clone $GIT_FLAGS -b v1.66.1 --depth 1 "${GITHUB_BASE_URL}/grpc/grpc"
  echo "*** Cloning grpc DONE"
fi
if [ "$BUILD_GRPC" = "yes" ]; then
  echo
  echo "*** Building grpc"
  cd $SRC/grpc
  mkdir -p "cmake/$BUILD_DIR" && cd "cmake/$BUILD_DIR"
  cmake -DCMAKE_CXX_STANDARD=17 \
        ${cmake_common_args} \
        -DCMAKE_INSTALL_PREFIX=$(prefix grpc) \
        -DgRPC_INSTALL=ON \
        -DgRPC_ABSL_PROVIDER=package \
        -DgRPC_CARES_PROVIDER=package \
        -DgRPC_PROTOBUF_PROVIDER=package \
        -DgRPC_RE2_PROVIDER=package \
        -DgRPC_SSL_PROVIDER=package \
        -DgRPC_ZLIB_PROVIDER=package \
        -DgRPC_BUILD_CSHARP_EXT=NO \
        ../..
  make -j$NCPUS
  make install
  cd ../.. && rm -fr "cmake/$BUILD_DIR"
  if [ "$clean" = "yes" ]; then
    rm -fr "$SRC/grpc"
  fi
  echo "*** Building grpc DONE"
fi

### arrow
if [ "$CLONE_ARROW" = "yes" ]; then
  echo
  echo "*** Cloning arrow"
  cd $SRC
  # Previously used version: apache-arrow-16.0.0
  git clone $GIT_FLAGS -b apache-arrow-16.1.0 --depth 1 "${GITHUB_BASE_URL}/apache/arrow"
  echo "*** Cloning arrow DONE"
fi
if [ "$BUILD_ARROW" = "yes" ]; then
  if [ "$shared" = "yes" ]; then
    cmake_arrow_extra_args="-DARROW_BUILD_STATIC=ON -DARROW_BUILD_SHARED=ON"
  else
    cmake_arrow_extra_args="-DARROW_BUILD_STATIC=ON -DARROW_BUILD_SHARED=OFF"
  fi
  echo
  echo "*** Building arrow"
  export CPATH=${PFX}/abseil/include${CPATH+:$CPATH}
  export CPATH=${PFX}/protobuf/include${CPATH+:$CPATH}
  cd $SRC/arrow/cpp
  mkdir -p "$BUILD_DIR" && cd "$BUILD_DIR"
  cmake -DProtobuf_DIR=${PFX}/protobuf \
        -DCMAKE_CXX_STANDARD=17 \
        ${cmake_common_args} \
        ${cmake_arrow_extra_args} \
        -DARROW_FLIGHT=ON \
        -DARROW_CSV=ON \
        -DARROW_FILESYSTEM=ON \
        -DARROW_COMPUTE=OFF \
        -DARROW_DATASET=OFF \
        -DARROW_PARQUET=ON \
        -DARROW_WITH_BZ2=ON \
        -DARROW_WITH_ZLIB=ON \
        -DARROW_WITH_LZ4=ON \
        -DARROW_WITH_SNAPPY=ON \
        -DARROW_WITH_ZSTD=ON \
        -DARROW_WITH_BROTLI=ON \
	-DARROW_SIMD_LEVEL=NONE \
        -DCMAKE_INSTALL_PREFIX=$(prefix arrow) \
        ..
  make -j$NCPUS
  make install
  cd .. && rm -fr "$BUILD_DIR"
  if [ "$clean" = "yes" ]; then
    rm -fr "$SRC/arrow"
  fi
  echo "*** Building arrow DONE"
  unset cmake_extra_arrow_args
fi

### immer
if [ "$CLONE_IMMER" = "yes" ]; then
  echo
  echo "*** Clone immer"
  cd $SRC
  # Previously used version: SHA e5d79ed80ec74d511cc4f52fb68feeac66507f2c.
  git clone $GIT_FLAGS -b v0.8.1 "${GITHUB_BASE_URL}/arximboldi/immer.git"
  echo "*** Clonning immer DONE"
fi
if [ "$BUILD_IMMER" = "yes" ]; then
  echo
  echo "*** Building immer"
  cd $SRC/immer
  mkdir -p "$BUILD_DIR" && cd "$BUILD_DIR"
  cmake ${cmake_common_args} \
        -DCMAKE_INSTALL_PREFIX=$(prefix immer) \
        -DCMAKE_CXX_STANDARD=17 \
        -Dimmer_BUILD_EXTRAS=OFF \
        -Dimmer_BUILD_TESTS=OFF \
        -Dimmer_BUILD_EXAMPLES=OFF \
        ..
  make -j$NCPUS
  make install
  cd .. && rm -fr "$BUILD_DIR"
  if [ "$clean" = "yes" ]; then
    rm -fr "$SRC/immer"
  fi
  echo "*** Building immer DONE"
fi

echo DONE.
echo

if [ "$GENERATE_ENV" = "yes" ]; then
  echo -n "Creating env.sh..."
  cd $DHDEPS_HOME
  (
# Note use of double or single quotes below to distinguish between the need
# for either evaluating now or delaying evaluation.
   echo "DHCPP=\"$DHDEPS_HOME\"; export DHCPP"
   echo "CMAKE_PREFIX_PATH=\"$CMAKE_PREFIX_PATH\"; export CMAKE_PREFIX_PATH"
   echo 'NCPUS=`getconf _NPROCESSORS_ONLN`; export NCPUS'
   if [ "$shared" = "yes" ]; then
     echo "LD_LIBRARY_PATH=\"$LD_LIBRARY_PATH\"; export LD_LIBRARY_PATH"
   fi
  ) > env.sh
  echo DONE.
fi

exit 0
