# Native packaging for Deephaven Netty server

This build is not actively maintained or tested, but should still work. As this build uses the grpc-netty server
implementation, it can not be used for serving static web content and does not provide any grpc-web proxies. As
such, it can only be used in an environment where the Deephaven Web IDE will not be used, or where the IDE is
hosted on a different server, with some grpc-web proxy (such as [Envoy](https://www.envoyproxy.io/)) between the
Deephaven server and the browser.

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

See [the configuration section for the jetty-app](../jetty-app/README.md#configuration) for flags that are supported here.

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
