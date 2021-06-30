# gRPC api Groovy console example

### Build

```shell
./gradlew grpc-api-client-console:build
```

produces

* `grpc-api-client/console/build/distributions/grpc-api-client-console-<version>.tar`
* `grpc-api-client/console/build/distributions/grpc-api-client-console-<version>.zip`

### Run

Install and run locally

```shell
./gradlew grpc-api-client-console:installDist

# update the endpoint as appropriate
./grpc-api-client/console/build/install/grpc-api-client-console/bin/console-groovy localhost:10000
```

```shell
./grpc-api-client/console/build/install/grpc-api-client-console/bin/console-python localhost:10000
```

or, run via IJ (todo, describe setup).
