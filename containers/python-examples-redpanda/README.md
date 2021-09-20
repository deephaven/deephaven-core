
# Overview

A Docker image for [Deephaven](https://deephaven.io/core/docs/tutorials/quickstart), [Deephaven Examples Repository](https://github.com/deephaven/examples) with [redpanda](https://github.com/vectorizedio/redpanda).

## Why this is needed

This project will open several docker containers including:
 - grpc-api
 - web
 - grpc-proxy
 - envoy
 - examples
 - redpanda
 - apicurio-registry-mem

## Run a Docker-build job

For full instructions to integrate [kafka with Deephaven](https://deephaven.io/core/docs/conceptual/kafka-in-deephaven/) see our [simple guide](https://deephaven.io/core/docs/how-to-guides/kafka-simple/).

To run these images execute the following in the directory of your choice:
```

compose_file=https://raw.githubusercontent.com/deephaven/deephaven-core/main/containers/python-examples-redpanda/docker-compose.yml
curl  -O "${compose_file}"

docker-compose pull

docker-compose up -d
```
