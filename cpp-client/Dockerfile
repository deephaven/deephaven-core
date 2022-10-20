#
# Copyright (c) Deephaven Data Labs. All rights reserved.
#

FROM ubuntu:22.04
ARG DEBIAN_FRONTEND=noninteractive

#
# Install system dependencies (eg, build tools) for build.
#
RUN set -eux; \
    apt-get -qq update && \
    apt-get -qq -y --no-install-recommends install \
            locales \
            ca-certificates \
            curl \
            git \
            g++ \
            cmake \
            make \
            build-essential \
            zlib1g-dev \
            libssl-dev; \
    echo "en_US.UTF-8 UTF-8" >> /etc/locale.gen; \
    locale-gen en_US.UTF-8; \
    apt-get clean; \
    rm -rf /var/lib/apt/lists/*

#
# Download and build dependent libraries, each as its own separate step.
#
WORKDIR /cpp-client/deps
# Be mindful about the order below:
# 1. Some libraries have cross dependencies (eg, arrow depends on grpc).
# 2. If something fails, the docker cache at that point gets invalidated;
#    so it makes sense to put more brittle stuff later on.
# We mount bind below to avoid copying over a script that we don't want to linger in the image.
RUN --mount=type=bind,source=build-dependencies.sh,target=/build-dependencies.sh \
    set -eux; bash -c "/build-dependencies.sh --clean protobuf"
RUN --mount=type=bind,source=build-dependencies.sh,target=/build-dependencies.sh \
    set -eux; bash -c "/build-dependencies.sh --clean re2"
RUN --mount=type=bind,source=build-dependencies.sh,target=/build-dependencies.sh \
    set -eux; bash -c "/build-dependencies.sh --clean protobuf"
RUN --mount=type=bind,source=build-dependencies.sh,target=/build-dependencies.sh \
    set -eux; bash -c "/build-dependencies.sh --clean gflags"
RUN --mount=type=bind,source=build-dependencies.sh,target=/build-dependencies.sh \
    set -eux; bash -c "/build-dependencies.sh --clean absl"
RUN --mount=type=bind,source=build-dependencies.sh,target=/build-dependencies.sh \
    set -eux; bash -c "/build-dependencies.sh --clean cares"
RUN --mount=type=bind,source=build-dependencies.sh,target=/build-dependencies.sh \
    set -eux; bash -c "/build-dependencies.sh --clean zlib"
RUN --mount=type=bind,source=build-dependencies.sh,target=/build-dependencies.sh \
    set -eux; bash -c "/build-dependencies.sh --clean grpc"
RUN --mount=type=bind,source=build-dependencies.sh,target=/build-dependencies.sh \
    set -eux; bash -c "/build-dependencies.sh --clean immer"
RUN --mount=type=bind,source=build-dependencies.sh,target=/build-dependencies.sh \
    set -eux; bash -c "/build-dependencies.sh --clean arrow"
RUN --mount=type=bind,source=build-dependencies.sh,target=/build-dependencies.sh \
    set -eux; bash -c "/build-dependencies.sh env"

#
# Build client library.
#
RUN mkdir -p /cpp-client/deephaven/client
COPY deephaven/CMakeLists.txt /cpp-client/deephaven
COPY deephaven/client /cpp-client/deephaven/client/

RUN mkdir -p /cpp-client/deephaven/build
WORKDIR /cpp-client/deephaven/build
RUN /bin/bash -c "set -eux; source /cpp-client/deps/env.sh; \
  cmake -DCMAKE_INSTALL_PREFIX=/cpp-client/install .. && make -j\$NCPUS install clean"

#
# Build tests.
#
COPY tests/*.cc tests/*.h tests/CMakeLists.txt /cpp-client/tests/
COPY tests/third_party /cpp-client/tests/third_party/

RUN mkdir -p /cpp-client/tests/build
WORKDIR /cpp-client/tests/build
RUN /bin/bash -c "set -eux; source /cpp-client/deps/env.sh; \
  cmake -DCMAKE_PREFIX_PATH=/cpp-client/install:\$CMAKE_PREFIX_PATH .. && make -j\$NCPUS"

COPY cpp-tests-to-junit.sh /cpp-client
#ENTRYPOINT bash -c "cd /cpp-client && ./cpp-tests-to-junit.sh test-output.xml"
