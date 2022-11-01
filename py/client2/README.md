Client using Cython-wrapped C++ code.

To build the code in this directory, you need a python environment with cython and numpy.
For instance, in Ubuntu 22.04 I created a python venv like so:

```
mkdir ~/py
python3 -m venv ~/py/cython
source ~/py/cython/bin/activate
# From now on your prompt will print '(cython)' in a separate line
# at the end of every command, to remind you you are executing inside
# the venv; to exit the venv just type "deactivate" any time.
#
# Any pip3 installs we do will happen inside the active venv.
# pip3 install numpy
# pip3 install cython
```

Set the `CMAKE_PREFIX_PATH` environment variable to include all DH's cpp-client dependencies.
Your `CMAKE_PREFIX_PATH` should look something like:

```
CMAKE_PREFIX_PATH=\
/home/cfs/dhcpp/local/abseil:\
/home/cfs/dhcpp/local/arrow:\
/home/cfs/dhcpp/local/boost:\
/home/cfs/dhcpp/local/cares:\
/home/cfs/dhcpp/local/flatbuffers:\
/home/cfs/dhcpp/local/gflags:\
/home/cfs/dhcpp/local/grpc:\
/home/cfs/dhcpp/local/immer:\
/home/cfs/dhcpp/local/protobuf:\
/home/cfs/dhcpp/local/re2:\
/home/cfs/dhcpp/local/zlib
```

(if you used `build-dependencies.sh` to build them, the script created a `env.sh` script for you
that you can source to set CMAKE_PREFIX_PATH to the targets it created for you).

Then run:

```
  $ python3 setup.py build_ext --inplace
```

Other environment variables useful for debugging:

* `VERBOSE=1` prints detailed compiler invocations, including flags like `-I`.
* `PARALLEL=9` run up to 9 parallel processes, useful to speed up the compilation.  Set the value to your number of CPUS + 1.
* `CMAKE_FLAGS=...` set to any desired value to pass down that flag to cmake; `--trace-expand` shows cmake decision as they are being made.

Once built, a shared object with the binary python module should show up under pydeephaven2, named like
`./pydeephaven2/pydeephaven2.cpython-310-x86_64-linux-gnu.so`.