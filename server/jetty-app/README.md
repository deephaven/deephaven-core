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
    $ export DEEPHAVEN_VERSION=0.17.0 # this should match the current version of your git repo

    $ cd py/jpy
    $ export JAVA_HOME=/path/to/your/java/home # Customize this to fit your computer
    $ python setup.py bdist_wheel
    $ pip install dist/deephaven_jpy-0.17.0-cp39-cp39-linux_x86_64.whl # This will vary by version/platform
    $ cd -

    $ cd Integrations/python
    $ python setup.py bdist_wheel
    $ pip install dist/deephaven-0.17.0-py2.py3-none-any.whl
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

By default, the server starts up on all interfaces with plaintext port 10000 (port 443 when SSL is enabled), a token
expiration duration of 5 minutes, a scheduler pool size of 4, and a max inbound message size of 100 MiB.

To bring up a SSL-enabled server on port 8443 with a development key and certificate, you can run:
```shell
./gradlew server-jetty-app:run -Pgroovy -PdevCerts
```

SSL configuration can be applied manually with the properties "ssl.identity.type", "ssl.identity.certChainPath",
"ssl.identity.privateKeyPath", "ssl.trust.type", and "ssl.trust.path". Furthermore, outbound Deephaven-to-Deephaven
connections can be explicitly configured separately if desired, with the same properties prefixed with "outbound.".
See the javadocs on `io.deephaven.server.jetty.JettyConfig` and `io.deephaven.server.runner.Main.parseSSLConfig` for
more information.

### SSL examples

#### Simple

```properties
ssl.identity.type=privatekey
ssl.identity.certChainPath=server.chain.crt
ssl.identity.privateKeyPath=server.key
```

This is a common setup where the server specifies a private key and certificate chain. The certificate can be signed
either by a public CA or an internal CA.

#### Intranet

```properties
ssl.identity.type=privatekey
ssl.identity.certChainPath=server.chain.crt
ssl.identity.privateKeyPath=server.key
ssl.trust.type=certs
ssl.trust.path=ca.crt
```

This is a common intranet setup where the server additionally specifies a trust certificate. Most often, this will be a
pattern used by organizations with an internal CA.

Outbound initiated Deephaven-to-Deephaven connections will trust other servers that can be verified via ca.crt or the
JDK trust stores.

#### Zero-trust / Mutual TLS

```properties
ssl.identity.type=privatekey
ssl.identity.certChainPath=server.chain.crt
ssl.identity.privateKeyPath=server.key
ssl.trust.type=certs
ssl.trust.path=ca.crt
ssl.clientAuthentication=NEEDED
```

This is a setup where the server additionally specifies that mutual TLS is required. This may be used to publicly expose
a server without the need to setup a VPN, or for high security intranet needs.

Inbound connections need to be verifiable via ca.crt. Outbound initiated Deephaven-to-Deephaven connections will trust
other servers that can be verified via ca.crt or the JDK trust stores.

#### Outbound

```properties
outbound.ssl.identity.type=privatekey
outbound.ssl.identity.certChainPath=outbound-identity.chain.crt
outbound.ssl.identity.privateKeyPath=outbound-identity.key
outbound.ssl.trust.type=certs
outbound.ssl.trust.path=outbound-ca.crt
```

In all of the above cases, the outbound Deephaven-to-Deephaven connections can be configured separately from the
server's inbound configuration.
