# Deephaven Python Ticking Package

The `pydeephaven-ticking` package enables access to ticking Deephaven data from Python. It extends
the functionality of the pydeephaven package to add the ability to subscribe to tables and receive
change notifications containing added, removed, and modified data. It uses Cython to create a thin
wrapper around Deephaven's C++ library.

## Prerequisites

The Deephaven C++ client (and ticking Python client) are tested regularly on Ubuntu 22.04 x86_64.
Additionally, successful tests have been run on RHEL 8 and Fedora 38 (both on x86_64).
Windows support is expected to be made available in the future.

## Installation

`pydeephaven-ticking` can be installed with `pip` or by building from source.

## `pip`

A Linux operating system on x86_64 architecture is required to install via `pip`.
It's recommended to install this way in a Python virtual environment (venv).

```sh
pip install pydeephaven-ticking
```

## Build from source

If building from source, `pydeephaven-ticking` also requires a working installation of the C++
client. All four packages (Deephaven Core, `pydeephaven`, `pydeephaven-ticking`, and the Deephaven 
C++ client) are present in the [deephaven-core](https://github.com/deephaven/deephaven-core) 
repository. It is assumed that you have the repository checked out at the location specified by 
`${DHROOT}`.

### Install the C++ client

First, install the Deephaven C++ client. Follow the instructions in `$DHROOT/cpp-client/README.md`.
Note the restrictions on supported platforms mentioned there. The instructions will ask you to
select a location for the installation of the C++ client library and its dependencies.  For the
purpose of this document we assume that location is specified in the `${DHCPP}` environment
variable.

### Install pydeephaven

To install pydeephaven, follow the instructions in `${DHROOT}/py/client/README.md`.

These instructions will require you to create a Python venv. After installing that package,
you will continue to use that venv here.

### Build the ticking Python client

#### Install Cython in the venv

If you've exited your venv, re-activate it with something like:

```sh
source ~/py/cython/bin/activate
```

Then run 

```sh
pip3 install cython
```

#### Build the shared library:

```sh
cd ${DHROOT}/py/client-ticking
```

```sh
# Ensure the DHCPP environment variable is set per the instructions above
rm -rf build dist  # Ensure we clean the remnants of any pre-existing build.
DEEPHAVEN_VERSION=$(../../gradlew :printVersion -q) CPPFLAGS="-I${DHCPP}/include" LDFLAGS="-L${DHCPP}/lib" python3 setup.py build_ext -i
```

#### Install pydeephaven-ticking

Build the wheel with

```sh
DEEPHAVEN_VERSION=$(../../gradlew :printVersion -q) python3 setup.py bdist_wheel
```

Then install the package.
Note: the actual name of the `.whl` file may be different depending on system details.

```sh
pip3 install --force --no-deps dist/pydeephaven_ticking-<x.y.z>-cp310-cp310-linux_x86_64.whl
```

The `--force` flag is required to overwrite any previously-built version of the package that might
already be there. The `--no-deps` flag ensures that we are sure to refer to the `pydeephaven`
package just built from the above steps, rather than one from PyPi.

## Testing the library

### Run tests
``` shell
$ python3 -m unittest discover tests

```

### Sample Python program

Run python from the venv while in this directory, and try this sample Python program:

``` python
import pydeephaven as dh
import time
session = dh.Session() # assuming Deephaven Community Core is running locally with the default configuration
table = session.time_table(period=1000000000).update(formulas=["Col1 = i"])
listener_handle = dh.listen(table, lambda update : print(update.added()))
listener_handle.start()
# data starts printing asynchronously here
time.sleep(10)
listener_handle.stop()
session.close()
```
