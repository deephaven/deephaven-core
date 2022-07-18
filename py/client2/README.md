Client using Cython-wrapped C++ code.

To build the code in this directory, you need a python environment with cython and numpy.
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

(if you used `build-dependencies.sh` to build them, the value is printed at the end).
Then run:

```
  $ python setup.py build
```
