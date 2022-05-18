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
    $ export DEEPHAVEN_VERSION=0.12.0 # this should match the current version of your git repo

    $ cd py/jpy
    $ export JAVA_HOME=/path/to/your/java/home # Customize this to fit your computer
    $ python setup.py bdist_wheel
    $ pip install dist/deephaven_jpy-0.12.0-cp39-cp39-linux_x86_64.whl # This will vary by version/platform
    $ cd -

    $ cd Integrations/python
    $ python setup.py bdist_wheel
    $ pip install dist/deephaven-0.12.0-py2.py3-none-any.whl
    $ cd -
    ```


### Build

```shell
./gradlew server-jetty-app:build
```

produces

* `server/jetty-app/build/distributions/server-jetty-<version>.tar`
* `server/jetty-app/build/distributions/server-jetty-<version>.zip`

### Run

The above artifacts can be uncompressed and their `bin/start` script can be executed:

```shell
 JAVA_OPTS="-Ddeephaven.console.type=groovy" bin/start
```

Alternatively, the uncompressed installation can be built directly by gradle:

```shell
./gradlew server-jetty-app:installDist
```

And then run via:

```shell
JAVA_OPTS="-Ddeephaven.console.type=groovy" ./server/jetty-app/build/install/server-jetty/bin/start
```

Finally, Gradle can be used to update the build and run the application in a single step:

```shell
./gradlew server-jetty-app:run -Pgroovy
```

### Internals

`server-jetty-app` is configured by default to include code that depends on JVM internals via
`--add-opens java.management/sun.management=ALL-UNNAMED`. To disable this, set the gradle property `includeHotspotImpl`
to `false`.

### Configuration / SSL

The jetty server can be configured via a JSON file specified via `-Dserver.config=file.json`. By default, the server
starts up on all interfaces with plaintext port 10000 (port 443 when SSL is enabled), a token expiration duration of
5 minutes, a scheduler pool size of 4, and a max inbound message size of 100 MiB. This is represented by the following
JSON:

```json
{
   "host": "0.0.0.0",
   "port": 10000,
   "tokenExpire": "PT5m",
   "schedulerPoolSize": 4,
   "maxInboundMessageSize": 104857600,
   "websockets": true
}
```

To bind to the local interface instead of all interfaces:
```json
{
   "host": "127.0.0.1"
}
```

To enable SSL, you can can add an `ssl` and `identity` section:
```json
{
   "ssl": {
      "identity": {
         "type": "privatekey",
         "certChainPath": "ca.crt",
         "privateKeyPath": "key.pem"
      }
   }
}
```

If your identity material is in the Java keystore format, you can specify that with the `keystore` identity type:
```json
{
   "ssl": {
      "identity": {
         "type": "keystore",
         "path": "keystore.p12",
         "password": "super-secret"
      }
   }
}
```

The SSL block provides further options for configuring SSL behavior:
```json
{
   "ssl": {
      "protocols": ["TLSv1.3"],
      "ciphers": ["TLS_AES_128_GCM_SHA256"]
   }
}
```

To bring up a SSL-enabled server on port 8443 with a development key and certificate, you can run:
```shell
./gradlew server-jetty-app:run -Pgroovy -PdevCerts
```

Please see the javadocs on io.deephaven.server.jetty.JettyConfig for more information.
