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

The netty server can be configured via a JSON file specified via `-Dserver.config=file.json`. By default, the server
starts up on all interfaces with plaintext port 8080 (port 443 when SSL is enabled), a token expiration duration of
5 minutes, a scheduler pool size of 4, and a max inbound message size of 100 MiB. This is represented by the following
JSON:

```json
{
   "host": "0.0.0.0",
   "port": 8080,
   "tokenExpire": "PT5m",
   "schedulerPoolSize": 4,
   "maxInboundMessageSize": 104857600
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
./gradlew server-netty-app:run -Pgroovy -PdevCerts
```

Please see the javadocs on io.deephaven.server.netty.NettyConfig for more information.
