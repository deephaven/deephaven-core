
# Overview

A Docker image for Deephaven with Python development, [Deephaven Examples Repository](https://github.com/deephaven/examples) and [Redpanda](https://github.com/vectorizedio/redpanda). See the [README](https://github.com/deephaven/deephaven-core/blob/main/README.md#launch-python) for launch instructions.

## Why this is needed

This project will open several Docker containers, including:

 - grpc-api
 - web
 - grpc-proxy
 - envoy
 - examples
 - redpanda
 - apicurio-registry-mem

## Run a Docker-build job

For instructions to integrate [Kafka with Deephaven](https://deephaven.io/core/docs/conceptual/kafka-in-deephaven/), see our [simple guide](https://deephaven.io/core/docs/how-to-guides/kafka-simple/).

To run these images, execute the following in the directory of your choice:

```bash
compose_file=https://raw.githubusercontent.com/deephaven/deephaven-core/main/containers/python-examples-redpanda/docker-compose.yml
curl  -O "${compose_file}"

docker-compose pull

docker-compose up -d
```
