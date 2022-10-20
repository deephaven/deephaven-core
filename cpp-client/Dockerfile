#
# Copyright (c) Deephaven Data Labs. All rights reserved.
#

FROM ubuntu:jammy AS dh-cpp-client

#
# Install system dependencies (eg, build tools) for build.
#
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && \
    apt-get install -y \
            curl \
            git \
            g++ \
            cmake \
            make \
            build-essential \
            zlib1g-dev \
            libssl-dev && \
    apt-get clean

#
# Download and build dependent libraries.
#
RUN mkdir -p /cpp-client/deps
COPY build-dependencies.sh /cpp-client/deps
WORKDIR /cpp-client/deps
RUN bash -c "/cpp-client/deps/build-dependencies.sh && rm -fr /cpp-client/deps/src"

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
