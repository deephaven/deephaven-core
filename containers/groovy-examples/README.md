# Overview

A Docker Compose deployment for [Deephaven](https://deephaven.io).

## Features

- [Deephaven](https://deephaven.io)
- [Groovy](https://groovy-lang.org/) scripting
- [Deephaven Examples](https://github.com/deephaven/examples)

## Launch Deephaven

For launch instructions, see the [README](https://github.com/deephaven/deephaven-core#launch-groovy--java-with-example-data).  For full instructions to work with Deephaven, see the [Quickstart](https://deephaven.io/core/groovy/docs/tutorials/quickstart/).

To launch Deephaven, execute the following in your deployment directory:

```bash
curl -O https://raw.githubusercontent.com/deephaven/deephaven-core/main/containers/groovy-examples/docker-compose.yml
docker compose pull
docker compose up -d
```
