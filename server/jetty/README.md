# Native packaging for Deephaven Jetty server

### Setting up a Python virtual environment

This is an optional prerequisite, but lets the later commands to run the server work without
specifying groovy as the console language. If you skip this step, be sure to use groovy, or
there will be errors on startup indicating that the Python environment is not suitable.

See https://github.com/deephaven/deephaven-core/issues/1657 for more discussion

1. On MacOS there is an extra patch to apply at this time, to add an extra argument to clang,
see the first code snippet of the comment at
https://github.com/deephaven/deephaven-core/issues/1657#issuecomment-989040798 for specifics.
1. Make sure Python is installed with a shared library. For example, if using `pyenv install`,
be sure to first set `PYTHON_CONFIGURE_OPTS="--enabled-shared"`.
1. Make a new directory for a virtual environment, and set it up:
    ```shell
    $ mkdir dh-py && cd dh-py
    $ python -m venv local-jetty-build
    $ source local-jetty-build/bin/activate # this must be re-run for each new shell
    $ cd -
   ```
1. Build and install wheels for deephaven-jpy and deephaven:
    ```shell
    $ python -m pip install --upgrade pip # First upgrade pip
    $ pip install wheel
    $ export DEEPHAVEN_VERSION=0.9.0 # this should match the current version of your git repo

    $ cd py/jpy
    $ export JAVA_HOME=/path/to/your/java/home # Customize this to fit your computer
    $ python setup.py bdist_wheel
    $ pip install dist/deephaven_jpy-0.9.0-cp39-cp39-linux_x86_64.whl # This will vary by version/platform
    $ cd -

    $ cd Integrations/python
    $ python setup.py bdist_wheel
    $ pip install dist/deephaven-0.9.0-py2.py3-none-any.whl
    $ cd -
    ```


### Build

```shell
./gradlew server-jetty:build
```

produces

* `server/netty/build/distributions/server-jetty-<version>.tar`
* `server/netty/build/distributions/server-jetty-<version>.zip`

### Run

The above artifacts can be uncompressed and their `bin/start` script can be executed:

```shell
 JAVA_OPTS="-Ddeephaven.console.type=groovy" bin/start
```

Alternatively, the uncompressed installation can be built directly by gradle:

```shell
./gradlew server-jetty:installDist
```

And then run via:

```shell
JAVA_OPTS="-Ddeephaven.console.type=groovy" ./server/jetty/build/install/server-jetty/bin/start
```

Finally, Gradle can be used to update the build and run the application in a single step:

```shell
./gradlew :server-jetty:run -Pgroovy
```
