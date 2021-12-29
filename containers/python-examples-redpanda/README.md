# Overview

A Docker Compose deployment for [Deephaven](https://deephaven.io).

## Features

- [Deephaven](https://deephaven.io)
- [Python](https://python.org/) scripting
- [Deephaven Examples](https://github.com/deephaven/examples)
- [Redpanda](https://vectorized.io/), a Kakfa-compatible event streaming platform.

## Launch Deephaven

For instructions on launching and using Kafka, see [Kafka in Deephaven: an introduction](https://deephaven.io/core/docs/conceptual/kafka-in-deephaven/) and [Simple Kafka import](https://deephaven.io/core/docs/how-to-guides/kafka-simple/).  For full instructions to work with Deephaven, see the [Quick start](https://deephaven.io/core/docs/tutorials/quickstart).

To launch Deephaven, execute the following in your deployment directory:

```bash
compose_file=https://raw.githubusercontent.com/deephaven/deephaven-core/main/containers/python-examples-redpanda/docker-compose.yml
curl  -O "${compose_file}"

docker-compose pull
docker-compose up -d
```
