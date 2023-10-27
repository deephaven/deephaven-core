# Building the C++ client from a base Ubuntu 20.04 or 22.04 image

These instructions show how to install and run the Deephaven C++ client, its dependencies,
and its unit tests. We have tested these instructions in Ubuntu 22.04 with the default
C++ compiler and tool suite (cmake etc).  We have used the instructions in the past to build
for older Ubuntu versions (20.04) and for some Fedora versions, but we don't regularly test
on them anymore so we do notguarantee they are current for those platforms.

1. Start with an Ubuntu 22.04 install

2. Get Deephaven running by following the instructions here: https://deephaven.io/core/docs/how-to-guides/launch-build/

3. Get build tools
   ```
   sudo apt update
   sudo apt install curl git g++ cmake make build-essential zlib1g-dev libbz2-dev libssl-dev pkg-config
   ```

   See the notes at the end of this document if you need the equivalent packages for Fedora.

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
   at the correct version.
   You can download it directly from the link
   https://github.com/deephaven/deephaven-base-images/raw/fcef110f22e69849cbcac00b09128809a1d0786b/cpp-client/build-dependencies.sh
   (this script is also used from our automated tools, to generate a docker image to
   support tests runs; that's why it lives in a separate repo).
   The script downloads, builds and installs the dependent libraries
   (Protobuf, re2, gflags, absl, flatbuffers, c-ares, zlib, gRPC, and Arrow).
   Decide on a directory for the dependencies to live (eg, "$HOME/dhcpp").
   Create that directory and save the script there.

   The three main build types of a standard cmake build are supported,
   `Release`, `Debug` and `RelWithDebInfo`.  By default. `build-dependencies.sh`
   creates a `RelWithDebInfo` build.  To create a `Release` build, set the
   environment variable `BUILD_TYPE=Release` (1)

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
   wget https://github.com/deephaven/deephaven-base-images/raw/fcef110f22e69849cbcac00b09128809a1d0786b/cpp-client/build-dependencies.sh
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
   cmake \
       -DCMAKE_INSTALL_LIBDIR=lib \
       -DCMAKE_CXX_STANDARD=17 \
       -DCMAKE_INSTALL_PREFIX=${DHCPP} \
       -DCMAKE_BUILD_TYPE=RelWithDebInfo \
       -DBUILD_SHARED_LIBS=ON \
       .. \
     && \
       make -j$NCPUS install
   ```

   If you need `make` to generate detailed output of the commands it is running
   (which may be helpful for debugging the paths being used etc),
   you can set the environment variable `VERBOSE=1`.
   This is true in general of cmake-generated Makefiles.
   It is useful to keep the output of make for later reference
   in case you need to be sure of the exact compiler flags
   that were used to compile the library and its objects.
   You can tweak the last command above as

   ```
   VERBOSE=1 make -j$NCPUS install 2>&1 | tee make-install.log
   ```

8. Run one Deephaven example which uses the installed client.
   This is a smoke test that the basic functionality
   is working end to end, and the build is properly configured.
   Note this assumes Deephaven server is running (see step 2),
   and the build created on step 7 is available in the same directory.

   ```
   cd $DHSRC/deephaven-core/cpp-client/deephaven/build/examples
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

10. Building in different distributions or with older toolchains.
    While we don't support other linux distributions or GCC versions earlier
    than 11, this section provides some notes that may help you
    in that situation.

    * GCC 8 mixed with older versions of GNU as/binutils may fail to compile
      `roaring.c` with an error similar to:
      ```
      /tmp/cczCvQKd.s: Assembler messages:
      /tmp/cczCvQKd.s:45826: Error: no such instruction: `vpcompressb %zmm0,%zmm1{%k2}'
      /tmp/cczCvQKd.s:46092: Error: no such instruction: `vpcompressb %zmm0,%zmm1{%k1}'
      ```
      In that case, add `-DCMAKE_C_FLAGS=-DCROARING_COMPILER_SUPPORTS_AVX512=0`
      to the list of arguments to `cmake`.

    * Some platforms combining old versions of GCC and cmake may fail
      to set the cmake C++ standard to 17 without explicitly adding
      `-DCMAKE_CXX_STANDARD=17` to the list of arguments to `cmake`.
      Note the default mode for C++ is `-std=gnu++17` for GCC 11.

Notes
  (1) The standard assumptions for `Debug` and `Release` apply here.
      With a `Debug` build you get debug information which is useful during
      development and testing of your own code that depends on the client
      and indirectly on these libraries.  A `Release` build gives you
      optimized libraries that are faster and smaller but with no
      debugging information.  Note that while in general it is expected
      to be able to freely mix some `Debug` and `Release` code,
      some of the dependent libraries are incompatible; in particular,
      protobuf generates different code and code compiled for a `Release`
      target using protobuf header files will not link against a `Debug`
      version of protobuf.  To keep things simple, we suggest that you run
      a consistent setting for your code and all dependencies.
  (2) In Fedora, the packages needed for building:

      ```
      dnf -y groupinstall 'Development Tools'
      dnf -y install curl cmake gcc-c++ openssl-devel libcurl-devel
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
