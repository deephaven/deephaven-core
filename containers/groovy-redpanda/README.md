# Overview

## Features

- [Deephaven](https://deephaven.io)
- [Groovy](https://groovy-lang.org/) scripting
- [Redpanda](https://vectorized.io/), a Kafka-compatible event streaming platform

## Launch Deephaven

For instructions on launching and using Kafka in Deephaven, see[How to connect to a Kafka stream](https://deephaven.io/core/groovy/docs/how-to-guides/data-import-export/kafka-stream/). For instructions on getting started with Deephaven, see the [Quickstart](https://deephaven.io/core/groovy/docs/tutorials/quickstart).

To launch Deephaven, execute the following in your deployment directory:

```sh
curl -O https://raw.githubusercontent.com/deephaven/deephaven-core/main/containers/groovy-redpanda/docker-compose.yml
docker compose pull
docker compose up -d
```
