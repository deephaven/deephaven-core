# Deephaven Python Ticking Package

The pydeephaven-ticking package helps you accesss Deephaven ticking data from Python. It extends
the functionality of the pydeephaven package to add the ability to subscribe to tables and receive
change notifications containing added, removed, and modified data. This package uses Cython to
create a thin wrapper around the Deephaven native C++ library.

## Prerequisites

Before installing this library, you will need to install the Deephaven Core C++ library and the
pydeephaven python package. All three packages (Deephaven Core C++, pydeephaven, Deephaven Core,
and pydeephaven-ticking) are present in the Deephaven Core github repository. We assume you
have checked out this repository at the location specified by `${DHROOT}`.

### Installing Deephaven Core

To install the Deephaven Core C++ library, follow the instructions in `$DHROOT/cpp-client/README.md`.
Note the restrictions on supported platforms mentioned there. The instructions will ask you to
select a location for the installation of the C++ client library and its dependencies.  For the
purpose of this document we assume that location is specified in the `${DHCPP}` environment
variable.  On my computer `${DHCPP}` is `$HOME/dhcpp` (where `$HOME` points to my home directory).

### Installing pydeephaven

To install pydeephaven, follow the instructions in `${DHROOT}/py/client/README.md`.

These instructions will require you to create a python venv. After installing that package,
you will continue to use that venv here.


## Building the Deephaven ticking library for Python

### Install Cython in the venv

If you've exited your venv, re-activate it with something like:
```
source ~/py/cython/bin/activate
```

Then run 
```
pip3 install cython
```

### Build the shared library:

```
cd ${DHROOT}/py/client-ticking
```

```
# Ensure the DHCPP environment variable is set per the instructions above
rm -rf build  # Ensure we clean the remnants of any pre-existing build.
CFLAGS="-I${DHCPP}/local/include" LDFLAGS="-L${DHCPP}/local/lib" python setup.py build_ext -i
```

### Install pydeephaven-ticking

Build the wheel with

```
python3 setup.py bdist_wheel
```

Then install the package.
Note the actual name of the `.whl` file may be different depending on system details.

```
pip3 install --force --no-deps dist/pydeephaven_ticking-0.29.0-cp310-cp310-linux_x86_64.whl
```

The reason for the "--force" flag is to overwrite any previously-built version of the package that
might already be there. The reason for the "--no-deps" flag is so that we are sure to refer to the
`pydeephaven` package that you just built in the above steps, rather than pulling in a prebuilt
one from the PyPI repository.

## Testing the library

Run python from the venv while in this directory, and try this sample Python program:

```
import pydeephaven as dh
import time
session = dh.Session() # assuming Deephaven Community Edition is running locally with the default configuration
table = session.time_table(period=1000000000).update(formulas=["Col1 = i"])
listener_handle = dh.listen(table, lambda update : print(update.added()))
listener_handle.start()
# data starts printing asynchronously here
time.sleep(10)
listener_handle.stop()
session.close()
```
