# gRPC api client simple example

### Build

```shell
./gradlew grpc-api-client-simple:build
```

produces

* `grpc-api-client/simple/build/distributions/grpc-api-client-simple-<version>.tar`
* `grpc-api-client/simple/build/distributions/grpc-api-client-simple-<version>.zip`

### Run

```shell
# update the endpoint as appropriate
./gradlew grpc-api-client-simple:run --args="localhost:10000"
```

or, install and run locally

```shell
./gradlew grpc-api-client-simple:installDist

# update the endpoint as appropriate
./grpc-api-client/simple/build/install/grpc-api-client-simple/bin/grpc-api-client-simple localhost:10000
```

or, run via IJ (todo, describe setup).
