
# Overview

A Docker image for [Deephaven](https://deephaven.io/core/docs/tutorials/quickstart) with Python development and [Deephaven Examples Repository](https://github.com/deephaven/examples).

## Why this is needed

This project will open several docker containers including:
 - grpc-api
 - web
 - grpc-proxy
 - envoy
 - examples

## Run a Docker-build job

For full instructions to work with Deephaven see the [quickstart guide](https://deephaven.io/core/docs/tutorials/quickstart).

To run these images execute the following in the directory of your choice:

```
compose_file=https://raw.githubusercontent.com/deephaven/deephaven-core/main/containers/python-examples/docker-compose.yml
curl  -O "${compose_file}"

docker-compose pull

docker-compose up -d
```
