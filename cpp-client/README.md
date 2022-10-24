# Building the C++ client from a base Ubuntu 20.04 or 22.04 image

These instructions show how to install and run the Deephaven C++ client, its dependencies,
and its unit tests. We have tested these instructions in Ubuntu 20.04 and 22.04 with the default
C++ compiler and tool suite (cmake etc).

1. Start with an Ubuntu 20.04 or 22.04 install

2. Get Deephaven running by following the instructions here: https://deephaven.io/core/docs/how-to-guides/launch-build/

3. Get build tools
   ```
   sudo apt update
   sudo apt install curl git g++ cmake make build-essential zlib1g-dev libssl-dev
   ```
4. Make a new directory for the Deephaven source code and assign that directory
   to a temporary shell variable. This will make subsequent build steps easier.
   ```
   mkdir -p ~/src/deephaven && cd ~/src/deephaven && export DHSRC=`pwd`
   ```
5. Clone deephaven-core sources.
   ```
   cd $DHSRC
   git clone https://github.com/deephaven/deephaven-core.git
   ```

6. Build and install dependencies for Deephaven C++ client.

   Get the `build-dependencies.sh` script from Deephaven's base images repository
   at `https://github.com/deephaven/deephaven-base-images/blob/main/cpp-client`
   You can download it directly from the link
   `https://github.com/deephaven/deephaven-base-images/raw/main/cpp-client/build-dependencies.sh`
   (this script is also used from our automated tools, to generate a docker image to
   support tests runs; that's why it lives in a separate repo).
   The script downloads, builds and installs the dependent libraries
   (Protobuf, re2, gflags, absl, flatbuffers, c-ares, zlib, gRPC, and Arrow).
   Edit your local copy of the script if necessary to reflect your selection
   of build tools and build target;
   defaults point to Ubuntu system's g++, cmake, and a Debug build target for cmake.
   Comments in the script will help you in identifying customization points.
   Decide on a directory for the dependencies to live (eg, "$HOME/dhcpp").
   Create that directory, and run your local copy of `build-dependencies.sh` from
   that directory.

   Example:
   ```
   mkdir -p $HOME/dhcpp
   cd $HOME/dhcpp
   wget https://github.com/deephaven/deephaven-base-images/raw/main/cpp-client/build-dependencies.sh
   # Maybe edit build-dependencies.sh to reflect choices of build tools and build target
   ./build-dependencies.sh
   ```

7. Build and install Deephaven C++ client

   ```
   cd $DHSRC/deephaven-core/cpp-client/deephaven/
   mkdir build && cd build
   export PFX=$HOME/dhcpp/local  # This should reflect your selection in the previous point.
   export CMAKE_PREFIX_PATH=${PFX}/abseil:${PFX}/boost:${PFX}/cares:${PFX}/flatbuffers:${PFX}/gflags:${PFX}/immer:${PFX}/protobuf:${PFX}/re2:${PFX}/zlib:${PFX}/grpc:${PFX}/arrow:${PFX}/deephaven
   export NCPUS=$(getconf _NPROCESSORS_ONLN)
   cmake -DCMAKE_INSTALL_PREFIX=${PFX}/deephaven .. && make -j$NCPUS install
   ```

8. Build and run the deephaven example which uses the installed client.
   Note this assumes deephaven server is running (see step 2).

   ```
   cd $DHSRC/deephaven-core/cpp-examples/hello_world
   mkdir build && cd build
   cmake .. && make -j$NCPUS
   ./hello_world
   ```

9. (Optional) run the unit tests

    ```
    cd $DHSRC/deephaven-core/cpp-client/tests
    mkdir build && cd build
    cmake .. && make -j$NCPUS
    ./tests
    ```
