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
   export DHSRC=$HOME/src/deephaven
   mkdir -p $DHSRC
   cd $DHSRC
   ```

5. Clone deephaven-core sources.
   ```
   cd $DHSRC
   git clone https://github.com/deephaven/deephaven-core.git
   ```

6. Build and install dependencies for Deephaven C++ client.

   Get the `build-dependencies.sh` script from Deephaven's base images repository
   at https://github.com/deephaven/deephaven-base-images/blob/main/cpp-client
   You can download it directly from the link
   https://github.com/deephaven/deephaven-base-images/raw/main/cpp-client/build-dependencies.sh
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
   # This should reflect your selection for where dependencies will live
   export DHCPP=$HOME/dhcpp
   # If the directory already exists from a previous attempt, ensure is clean/empty
   mkdir -p $DHCPP
   cd $DHCPP
   wget https://github.com/deephaven/deephaven-base-images/raw/main/cpp-client/build-dependencies.sh
   chmod +x ./build-dependencies.sh
   # Maybe edit build-dependencies.sh to reflect choices of build tools and build target, if you
   # want anything different than defaults; defaults should work fine for supported platforms.
   ./build-dependencies.sh
   ```

7. Build and install Deephaven C++ client.  Running `build-dependencies.sh` should have
   created an `env.sh` file that we source below to set relevant environment variables for
   the build.

   ```
   source $DHCPP/env.sh
   cd $DHSRC/deephaven-core/cpp-client/deephaven/
   mkdir build && cd build
   cmake -DCMAKE_INSTALL_PREFIX=${DHCPP}/local/deephaven .. && make -j$NCPUS install
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
