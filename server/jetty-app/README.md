# server-jetty-app

This README is oriented towards getting a server up for local development in a development environment. From a development environment, users can modify source code towards contributing to the project, creating custom capabilities, and more.

If you wish to use Deephaven from a production environment, which is simpler but source code cannot be modified, see one of the following documents:

- [Quickstart for Docker](https://deephaven.io/core/docs/tutorials/quickstart/)
- [How to configure the Deephaven native application](https://deephaven.io/core/docs/how-to-guides/configuration/native-application/)

This README deals with general development for either the Python or Groovy server-side API. For Python-specific development instructions, see the [Python development README](../../py/README.md).

## Local development

`./gradlew server-jetty-app:run` will incorporate local Java changes on each start. If you are not frequently changing Java code, see the next section.

```shell
./gradlew server-jetty-app:run # Python session (default)
./gradlew server-jetty-app:run -Pdebug # Attach a Java debugger to the Python session on port 5005
./gradlew server-jetty-app:run -Pgroovy # Groovy session
./gradlew server-jetty-app:run -Pgroovy -Pdebug # Attach a Java debugger to the Groovy session on port 5005
```

## Development with infrequent changes

To create a more production-like environment, you can create and invoke the start script instead of running via gradle. This is faster if you need to often restart the server without making any changes to Java code (such as Python server development).

```shell
./gradlew server-jetty-app:installDist # Run after any Java changes
./server/jetty-app/build/install/server-jetty/bin/start
```

### Configuration

The `START_OPTS` environment variable is used to set JVM arguments. For example:

```shell
START_OPTS="-Xmx12g" ./gradlew server-jetty-app:run # Starts Deephaven with 12gb of heap memory
```

While configuration properties can be inherited via JVM system properties (`-Dmy.property=my.value`), you may prefer to
set persistent configuration properties in the `<configDir>/deephaven.prop` file.
On Linux, this file is `~/.config/deephaven/deephaven.prop`.
On Mac OS, this file is `~/Library/Application Support/io.Deephaven-Data-Labs.deephaven/deephaven.prop`.

See [config-dir](https://deephaven.io/core/docs/how-to-guides/configuration/native-application/#config-directory) for more information on `<configDir>`.

See [config-file](https://deephaven.io/core/docs/how-to-guides/configuration/config-file/) for more information on the configuration file format.

### Shutdown

There are multiple ways to shut down the Deephaven server. The easiest is to `ctrl+C` the process. If it's being run in background mode, you can kill it with a `SIGINT`.

```sh
kill -2 <pid>
```

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