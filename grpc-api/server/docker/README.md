# Docker image deephaven/grpc-api

### Prerequisites
* docker

### Build

```shell
./gradlew grpc-api-server-docker:build
```

### Run

```shell
docker run --rm deephaven/grpc-api:latest
```

To run with specific memory:

```shell
docker run -m 100M --rm deephaven/grpc-api:latest
```

To run with debug logging:

```shell
docker run -e JAVA_TOOL_OPTIONS="-Dlogback.configurationFile=logback-debug.xml" --rm deephaven/grpc-api:latest
```

### TODO
More complete examples with docker-compose.