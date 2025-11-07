# Overview

A Docker Compose deployment for [Deephaven](https://deephaven.io).

## Features

- [Deephaven](https://deephaven.io)
- [Python](https://python.org/) scripting
- [Deephaven Examples](https://github.com/deephaven/examples)
- [Redpanda](https://vectorized.io/), a Kakfa-compatible event streaming platform

## Launch Deephaven

For instructions on launching and using Kafka in Deephaven, see[How to connect to a Kafka stream](https://deephaven.io/core/groovy/docs/how-to-guides/data-import-export/kafka-stream/). For instructions on getting started with Deephaven, see the [Quickstart](https://deephaven.io/core/docs/getting-started/quickstart/).

To launch Deephaven, execute the following in your deployment directory:

```bash
curl -O https://raw.githubusercontent.com/deephaven/deephaven-core/main/containers/python-examples-redpanda/docker-compose.yml
docker compose pull
docker compose up -d
```
