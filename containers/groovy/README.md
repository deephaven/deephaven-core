
# Overview

A Docker image for Deephaven with Groovy development. See the [README](https://github.com/deephaven/deephaven-core/blob/main/README.md#launch-groovy--java) for launch instructions.

## Why this is needed

This project will open several docker containers, including:
 - grpc-api
 - web
 - grpc-proxy
 - envoy

## Run a Docker-build job

For full instructions to work with Deephaven, see the [Quick start](https://deephaven.io/core/docs/tutorials/quickstart).

To run these images execute the following in the directory of your choice:

```bash
compose_file=https://raw.githubusercontent.com/deephaven/deephaven-core/main/containers/groovy/docker-compose.yml
curl  -O "${compose_file}"

docker-compose pull

docker-compose up -d
```
