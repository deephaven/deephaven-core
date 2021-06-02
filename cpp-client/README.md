# Building the C++ client from a base Ubuntu 20.04 image

These instructions show how to install and run the Deephaven C++ client, its dependencies,
and its unit tests. For now the installation instructions are limited to a static library
installation, into /usr/local, assuming C++17. We intend to provide instructions for other
variations in the future.

1. Start with an Ubuntu 20.04 install
2. Get build tools
   ```
   sudo apt update
   sudo apt install git g++ cmake make build-essential zlib1g-dev libssl-dev
   ```
3. Docker config for Deephaven
   ```
   sudo apt install openjdk-8-jdk docker
   ```
   1. So you don’t have to be root
      ```
      sudo groupadd docker
      sudo usermod -aG docker $USER
      “Log out and then back in” so your shell will recognize the new group
      ```       
   2. Get the specific version of docker-compose that is needed
      ```
      sudo curl -L "https://github.com/docker/compose/releases/download/1.29.2/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
      sudo chmod +x /usr/local/bin/docker-compose
      ```
4. Make a new directory for the Deephaven source code
   ```
   mkdir ~/mysrc
   ```
5. Get the Deephaven source
   ```
   cd ~/mysrc
   git clone git@github.com:deephaven/deephaven-core.git
   ```
6. Build and start the database. Do this in a separate shell because it will take a while
   ```
   cd  ~/mysrc
   cd deephaven-core
   ./gradlew prepareCompose && docker-compose --env-file default_groovy.env up --build
   ```
7. Build and install zstd
   ```
   cd ~/mysrc
   git clone https://github.com/facebook/zstd.git
   cd zstd/build/cmake
   mkdir build && cd build
   cmake .. && make -j8 && sudo make install
   ```
8. Build and install re2
   ```
   cd ~/mysrc
   git clone https://github.com/google/re2.git
   cd re2
   mkdir build && cd build
   cmake .. && make -j8 && sudo make install
   ```
9. Build and install protoc
   ```
   cd ~/mysrc
   wget https://github.com/protocolbuffers/protobuf/releases/download/v3.17.3/protobuf-cpp-3.17.3.tar.gz
   tar xfz protobuf-cpp-3.17.3.tar.gz
   cd protobuf-3.17.3
   cd cmake
   mkdir build && cd build
   cmake .. && make -j8 && sudo make install
   ```
10. Build and install grpc and absl
    ```
    cd ~/mysrc
    git clone --recurse-submodules -b v1.38.0 https://github.com/grpc/grpc
    cd grpc
    mkdir build && cd build
    cmake -DgRPC_INSTALL=ON  -DgRPC_BUILD_TESTS=OFF .. && make -j8 && sudo make install

    cd ../third_party/abseil-cpp
    mkdir build && cd build
    cmake -DCMAKE_POSITION_INDEPENDENT_CODE=TRUE .. && make -j8 && sudo make install
    ```
11. Build and install flatbuffers
   ```
   cd ~/mysrc
   git clone git@github.com:google/flatbuffers.git
   cd flatbuffers
   mkdir build && cd build
   cmake .. && make -j8 && sudo make install
   ```
12. Build and install Apache Arrow
    ```
    cd ~/mysrc
    # Arrange to get apache-arrow-5.0.0.tar.gz from https://arrow.apache.org/install/
    # then...
    tar xfz apache-arrow-5.0.0.tar.gz
    cd apache-arrow-5.0.0
    cd cpp
    mkdir build && cd build
    cmake -DARROW_BUILD_STATIC=ON -DARROW_FLIGHT=ON .. && make -j8 && sudo make install
    ```
13. Build and install the deephaven C++ client
    ```
    cd ~/mysrc/deephaven-core/cpp-client/deephaven/
    mkdir build && cd build
    cmake .. && make -j8 && sudo make install
    ```
14. Build and run the deephaven example which uses the installed client
    ```
    cd ~/mysrc/deephaven-core/cpp-client/simple-example
    mkdir build && cd build
    cmake .. && make -j8
    ./example_using_installed_dh  # run the program
    ```
15. Run the unit tests
    ```
    cd ~/mysrc/deephaven-core/cpp-client/tests
    mkdir build && cd build
    cmake .. && make -j8
    ./tests
    ```
