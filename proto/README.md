# Deephaven Protobuf

Deephaven's [protobuf](https://protobuf.dev/) files are what drives its client APIs such as [Python](https://deephaven.io/core/client-api/python/), [C++](https://deephaven.io/core/client-api/cpp-examples/getting_started.html), and more using [gRPC](https://grpc.io/). If you wish to build your own client API or change how all client APIs interact with the Deephaven server, this is the place to start.

For Java, proto changes will be automatically reflected when building/running the Java server app. Sections below list files that must be manually generated and committed to source control when proto files change.

## Python/Go/C++ API

```sh
./gradlew :py-client:updateProtobuf :go:updateProtobuf :cpp-client:updateProtobuf
```

## Authorization

See [authorization readme](../authorization/README.md)

## JS API

