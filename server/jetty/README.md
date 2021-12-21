# Native packaging for Deephaven Jetty server

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
