# server-jetty-app

## Native development

This document is oriented towards getting a server up for local development.
If you are interested in running a native production release, please see
[https://deephaven.io/core/docs/how-to-guides/configuration/native-application/](https://deephaven.io/core/docs/how-to-guides/configuration/native-application/).

### Groovy Quickstart

```shell
./gradlew server-jetty-app:run -Pgroovy 
```

### Python Quickstart

1. Setup virtual environment:

   ```shell
   python -m venv /tmp/my-dh-venv
   source /tmp/my-dh-venv/bin/activate
   ```

1. Build and install wheel

   ```shell
   ./gradlew py-server:assemble

   # replace with the appropriate <version>
   pip install "py/server/build/wheel/deephaven_core-<version>-py3-none-any.whl[autocomplete]"

   # To install without the optional `autocomplete` feature, run:
   # pip install "py/server/build/wheel/deephaven_core-<version>-py3-none-any.whl"
   ```

1. Run

   ```shell
   ./gradlew server-jetty-app:run
   ```

**Note:**

* This is not an exhaustive guide to managing python environments
* Depending on your OS and how your PATH is setup, you may need to use `python3`, or a path to the explicit python version you want to use
* You may choose to setup a "permanent" virtual environment location
* You'll need to re-install the wheel anytime you are making python code changes that affect the wheel
* `pip` can be a pain if you are trying to (re-)install a wheel with the same version number as before
  * A `pip install --force-reinstall --no-deps "py/server/build/wheel/deephaven_core-<version>-py3-none-any.whl[autocomplete]"` may do the trick
* You can install other python packages in your venv using `pip install <some-other-package>`
* You can setup multiple virtual environments, and switch between them as necessary using `source /path/to/other-venv/bin/activate`
* You can de-activate the virtual environment by running `deactivate`
* You can use the `VIRTUAL_ENV` environment variable instead of sourcing / activating virtual environments: `VIRTUAL_ENV=/my/venv ./gradlew server-jetty-app:run`

### Start script

To create a more production-like environment, you can create and invoke the start script instead of running via gradle:

```shell
./gradlew server-jetty-app:installDist
./server/jetty-app/build/install/server-jetty/bin/start
```


See [https://deephaven.io/core/docs/how-to-guides/configuration/native-application/](https://deephaven.io/core/docs/how-to-guides/configuration/native-application/)
for options when invoking the start script.

### Configuration

The `START_OPTS` environment variable is used to set JVM arguments. For example:

```shell
START_OPTS="-Xmx12g" ./gradlew server-jetty-app:run
```

While configuration properties can be inherited via JVM system properties (`-Dmy.property=my.value`), you may prefer to
set persistent configuration properties in the `<configDir>/deephaven.prop` file.
On Linux, this file is `~/.config/deephaven/deephaven.prop`.
On Mac OS, this file is `~/Library/Application Support/io.Deephaven-Data-Labs.deephaven/deephaven.prop`.

See [config-dir](https://deephaven.io/core/docs/how-to-guides/configuration/native-application/#config-directory) for more information on `<configDir>`.

See [config-file](https://deephaven.io/core/docs/how-to-guides/configuration/config-file/) for more information on the configuration file format.

### SSL

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
