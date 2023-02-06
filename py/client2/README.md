# Deephaven Python Client for ticking tables

The Deephaven Python Client enables you to interact with the Deephaven database via Python. The approach we use here is to use Cython to create a thin wrapper around the Deephaven native C++ library.

## Disclaimer

Because this is alpha software, this particular library only addresses the problem of accessing
*ticking* tables from Python. Accessing *static* tables is performed by a different library. We
are working on unifying these two libraries.


## Prerequisites

Clone the Deephaven Core repository. For the remainder of this document we will assume that your
clone is at the location specified by `${DHROOT}`.

First, you will need to build the Deephaven C++ library and its prerequisites. To do this, see
the file `${DHROOT}/cpp-client/README.md` in the Deephaven Core github repository.

For the purpose of this document we assume that you have successfully built the library and its prerequsites in a location specified by `${DHLIB}`. On my computer `${DHLIB}` is
`home/cfs/dhcpp/local`.

## Making the Python venv

To build the code in this directory, you need a python environment with cython and numpy.
For instance, in Ubuntu 22.04 I created a python venv and added cython and numpy to it like so:

```
mkdir ~/py
python3 -m venv ~/py/cython
source ~/py/cython/bin/activate
# From now on your prompt will print '(cython)' in a separate line
# at the end of every command, to remind you you are executing inside
# the venv; to exit the venv just type "deactivate" any time.
#
# Any pip3 installs we do will happen inside the active venv.
pip3 install numpy
pip3 install cython
```

## Building the Deephaven shared library for Python

First, enter the Python client directory:

```
cd ${DHROOT}/py/client2
```

Then run these commands to build the Deephaven shared library:

```
rm -rf build *.so && \
CFLAGS="-I${DHLIB}/deephaven/include" LDFLAGS="-L${DHLIB}/deephaven/lib -L${DHLIB}/protobuf/lib -L${DHLIB}/arrow/lib -L${DHLIB}/grpc/lib -L${DHLIB}/abseil/lib -L${DHLIB}/re2/lib" python setup.py build_ext -i
```

Once built, a shared object with the binary python module should show up, named like
`pydeephaven.cpython-38-x86_64-linux-gnu.so`.

## Testing the library

Most of the Deephaven client's library dependencies were linked statically and so they are already
included in the library. However, there are a couple that are linked dynamically. These need to be
included in your `LD_LIBRARY_PATH`:

```
export LD_LIBRARY_PATH=${DHLIB}/arrow/lib:${DHLIB}/cares/lib
```

Then run python from the venv while in this directory, and try this sample
program:
```
import deephaven_client as dh
client = dh.Client.connect("localhost:10000")
manager = client.getManager()
handle = manager.emptyTable(10).update(["II= ii"])
print(handle.toString(True))
```
