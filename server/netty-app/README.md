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
