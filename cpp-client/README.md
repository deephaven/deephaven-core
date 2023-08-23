# Building the C++ client from a base Ubuntu 20.04 or 22.04 image

These instructions show how to install and run the Deephaven C++ client, its dependencies,
and its unit tests. We have tested these instructions in Ubuntu 20.04 and 22.04 with the default
C++ compiler and tool suite (cmake etc).

1. Start with an Ubuntu 20.04 or 22.04 install

2. Get Deephaven running by following the instructions here: https://deephaven.io/core/docs/how-to-guides/launch-build/

3. Get build tools
   ```
   sudo apt update
   sudo apt install curl git g++ cmake make build-essential zlib1g-dev libssl-dev pkg-config
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
   Decide on a directory for the dependencies to live (eg, "$HOME/dhcpp").
   Create that directory and save the script there.

   Edit your local copy of the script if necessary to reflect your selection
   of build tools and build target;
   defaults point to Ubuntu system's g++, cmake, and a Debug build target for cmake.
   Comments in the script will help you in identifying customization points.
   Note however that defaults are tested by any deviation from defaults may require
   manual modification of other files later, when building the C++ client proper.

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
   # want anything different than defaults; defaults are tested to work,
   # any deviation from defaults may require changing other files later.
   # Below we save the output of the script to keep a record of the commands ran during the build.
   ./build-dependencies.sh 2>&1 | tee build-dependencies.log
   ```

   The execution of `build-dependencies.sh` will create, on the
   directory it is run, subdirectories `local` and `src` and a file
   `env.sh`.  The `local` directory will contain the installation of
   each dependent library (eg, `local/grpc` would be the location of
   the grpc installation, etc).  The `src` directory is where the
   sources for the libraries will be downloaded, cloned from their
   github repositories.  You can ask `build-dependencies.sh` to remove
   the downloaded sources once the libraries have been built by
   passing to it the `--clean` flag.  The default cmake build target is `Debug`,
   which would make it useful to keep the downloaded sources for
   debug/development purposes.  If `Release` versions are preferred,
   an environment variable `BUILD_TYPE` set to `Release` can be used
   to override the `Debug` default, but note that some libraries change their
   name (eg, `libprotobufd.a` versus `libprotobuf.a`) when compiled for
   a release version, which may conflict with assumptions elsewhere
   and may require manual modifications in other parts of the build
   (if this is your first time building, we suggest you stick with defaults).

   The `env.sh` file contains environment variable definitions in bourne
   shell syntax (compatible with bash) that will indicate to cmake
   the location of the libraries just built, via definition of a suitable
   `CMAKE_PREFIX_PATH`.  This file is intended to be `source`'d
   from a shell where you plan to build the C++ client.

7. Build and install Deephaven C++ client.  Running `build-dependencies.sh` should have
   created an `env.sh` file that we source below to set relevant environment variables for
   the build.

   ```
   source $DHCPP/env.sh
   cd $DHSRC/deephaven-core/cpp-client/deephaven/
   mkdir build && cd build
   cmake -DCMAKE_INSTALL_PREFIX=${DHCPP}/local \
       -DCMAKE_BUILD_TYPE=Debug -DBUILD_SHARED_LIBS=ON .. && \
     make -j$NCPUS install
   ```

8. Build and run the deephaven example which uses the installed client.
   Note this assumes deephaven server is running (see step 2),
   and the build created on step 7 is available in the same directory.

   ```
   cd $DHSRC/deephaven-core/cpp-client/deephaven/build/examples
   make -j$NCPUS
   cd hello_world
   ./hello_world
   ```

9. (Optional) run the unit tests
   This assumes the build created on step 7 is available in the same directory.

    ```
    cd $DHSRC/deephaven-core/cpp-client/deephaven/build/tests
    make -j$NCPUS
    ./tests
    ```

# Updating proto generated C++ stubs (intended for developers)
   1. Ensure you have a local installation of the dependent libraries
      as described earlier in this document.  Source the `env.sh`
      file to ensure you have the correct environment variable definitions
      in your shell for the steps below.

   2. In the `proto/proto-backplane-grpc/src/main/proto` directory
      (relative from your deephave-core clone base directory),
      run the `build-cpp-protos.sh` script.
      This should generate up-to-date versions of the C++ stubs
      according to the proto sources on the same clone.
