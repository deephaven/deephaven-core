# Native packaging for grpc-api

### Build

```shell
./gradlew grpc-api-server-native:build
```

produces

* `grpc-api/server/native/build/distributions/server-<version>.tar`
* `grpc-api/server/native/build/distributions/server-<version>.zip`

### Run

The above artifacts can be uncompressed and their `start` script can be executed.

Alternatively, the uncompressed installation can be built directly by gradle:

```shell
./gradlew grpc-api-server-native:installDist
```

And then run via:

```shell
JAVA_OPTS="-Ddeephaven.console.type=groovy" ./grpc-api/server/native/build/install/server/bin/start
```
