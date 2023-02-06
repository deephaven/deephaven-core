# Native packaging for Deephaven Netty server

### Build

```shell
./gradlew server-netty-app:build
```

produces

* `server/netty-app/build/distributions/server-<version>.tar`
* `server/netty-app/build/distributions/server-<version>.zip`

### Run

The above artifacts can be uncompressed and their `bin/start` script can be executed:

```shell
START_OPTS="-Ddeephaven.console.type=groovy" bin/start
```

Alternatively, the uncompressed installation can be built directly by gradle:

```shell
./gradlew server-netty-app:installDist
```

And then run via:

```shell
START_OPTS="-Ddeephaven.console.type=groovy" ./server/netty-app/build/install/server/bin/start
```

Finally, Gradle can be used to update the build and run the application in a single step:

```shell
./gradlew server-netty-app:run -Pgroovy
```

### Internals

`server-netty-app` is configured by default to include code that depends on JVM internals via
`--add-exports java.management/sun.management=ALL-UNNAMED`. To disable this, set the gradle property `-PexcludeHotspotImpl`.

`server-netty-app` is configured by default to include code that depends on JVM internals via
`--add-exports java.base/jdk.internal.misc=ALL-UNNAMED`. To disable this, set the gradle property `-PexcludeClockImpl`.

### Configuration / SSL

By default, the server starts up on all interfaces with plaintext port 8080 (port 443 when SSL is enabled), a token
expiration duration of 5 minutes, a scheduler pool size of 4, and a max inbound message size of 100 MiB.

To bring up a SSL-enabled server on port 8443 with a development key and certificate, you can run:
```shell
./gradlew server-netty-app:run -Pgroovy -PdevCerts
```

SSL configuration can be applied manually with the properties "ssl.identity.type", "ssl.identity.certChainPath",
"ssl.identity.privateKeyPath", "ssl.trust.type", and "ssl.trust.path". Furthermore, outbound Deephaven-to-Deephaven
connections can be explicitly configured separately if desired, with the same properties prefixed with "outbound.".
See the javadocs on `io.deephaven.server.netty.NettyConfig` and `io.deephaven.server.runner.Main.parseSSLConfig` for
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
