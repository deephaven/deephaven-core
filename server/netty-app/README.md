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
 JAVA_OPTS="-Ddeephaven.console.type=groovy" bin/start
```

Alternatively, the uncompressed installation can be built directly by gradle:

```shell
./gradlew server-netty-app:installDist
```

And then run via:

```shell
JAVA_OPTS="-Ddeephaven.console.type=groovy" ./server/netty-app/build/install/server/bin/start
```

Finally, Gradle can be used to update the build and run the application in a single step:

```shell
./gradlew server-netty-app:run -Pgroovy
```

### Internals

`server-netty-app` is configured by default to include code that depends on JVM internals via
`--add-opens java.management/sun.management=ALL-UNNAMED`. To disable this, set the gradle property `includeHotspotImpl`
to `false`.

### Configuration / SSL

By default, the server starts up on all interfaces with plaintext port 8080 (port 443 when SSL is enabled), a token
expiration duration of 5 minutes, a scheduler pool size of 4, and a max inbound message size of 100 MiB.

To bring up a SSL-enabled server on port 8443 with a development key and certificate, you can run:
```shell
./gradlew server-netty-app:run -Pgroovy -PdevCerts
```

SSL configuration can be applied manually with the properties "ssl.identity.type", "ssl.identity.certChainPath", and
"ssl.identity.privateKeyPath". See the javadocs on `io.deephaven.server.netty.NettyConfig` and
`io.deephaven.server.runner.Main.parseSSLConfig` for more information.
