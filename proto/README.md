# Deephaven Protobuf

For Java, proto changes will be automatically reflected when building/running the Java server app. Sections below list files that must be manually generated and committed to source control when proto files change.

## Python/Go/C++ API

```sh
./gradlew :py-client:updateProtobuf :go:updateProtobuf :cpp-client:updateProtobuf
```

## Authorization

See [authorization readme](../authorization/README.md)

## JS API

Needs info on how to generate