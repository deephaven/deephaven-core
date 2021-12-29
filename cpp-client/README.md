# Building the C++ client from a base Ubuntu 20.04 image

These instructions show how to install and run the Deephaven C++ client, its dependencies,
and its unit tests. For now the installation instructions are limited to a static library
installation, into /usr/local, assuming C++17. We intend to provide instructions for other
variations in the future.

1. Start with an Ubuntu 20.04 install
2. Get Deephaven running by following the instructions here: https://deephaven.io/core/docs/how-to-guides/launch-build/
3. Get build tools
   ```
   sudo apt update
   sudo apt install curl git g++ cmake make build-essential zlib1g-dev libssl-dev
   ```
4. Make a new directory for the Deephaven source code and assign that directory
   to a temporary shell variable. This will make subsequent build steps easier.
   ```
   mkdir ~/build_dh_client && cd build_dh_client && export MYSRC=`pwd`
   ```
5. Build and install protoc
   ```
   cd $MYSRC
   wget https://github.com/protocolbuffers/protobuf/releases/download/v3.17.3/protobuf-cpp-3.17.3.tar.gz
   tar xfz protobuf-cpp-3.17.3.tar.gz
   cd protobuf-3.17.3
   cd cmake
   mkdir build && cd build
   cmake .. && make -j8 && sudo make install
   ```
6. Build and install grpc and absl
    ```
    cd $MYSRC
    git clone --recurse-submodules -b v1.38.0 https://github.com/grpc/grpc
    cd grpc
    mkdir -p cmake/build && cd cmake/build
    cmake -DgRPC_INSTALL=ON -DgRPC_BUILD_TESTS=OFF ../.. && make -j8 && sudo make install

    cd ../../third_party/abseil-cpp
    mkdir -p cmake/build && cd cmake/build
    cmake -DCMAKE_POSITION_INDEPENDENT_CODE=TRUE ../.. && make -j8 && sudo make install
    ```
7. Build and install Apache Arrow
   ```
   cd $MYSRC
   wget 'https://www.apache.org/dyn/closer.lua?action=download&filename=arrow/arrow-5.0.0/apache-arrow-5.0.0.tar.gz' -O apache-arrow-5.0.0.tar.gz
   tar xfz apache-arrow-5.0.0.tar.gz
   cd apache-arrow-5.0.0/cpp
   mkdir build && cd build
   cmake -DARROW_BUILD_STATIC=ON -DARROW_FLIGHT=ON .. && make -j8 && sudo make install
   ```
8. Build and install the Deephaven C++ client
   ```
   cd $MYSRC
   git clone https://github.com/deephaven/deephaven-core.git
   cd deephaven-core/cpp-client/deephaven/
   mkdir build && cd build
   cmake .. && make -j8 && sudo make install
   ```
9. Build and run the deephaven example which uses the installed client
   ```
   cd $MYSRC/deephaven-core/cpp-examples/hello_world
   mkdir build && cd build
   cmake .. && make -j8
   ./hello_world
   ```
10. (Optional) run the unit tests
    ```
    cd ~/mysrc/deephaven-core/cpp-client/tests
    mkdir build && cd build
    cmake .. && make -j8
    ./tests
    ```
