# Overview

A Docker Compose deployment for [Deephaven](https://deephaven.io).

## Features

- [Deephaven](https://deephaven.io)
- [Python](https://python.org/) scripting
- [Deephaven Examples](https://github.com/deephaven/examples)

## Launch Deephaven

For launch instructions, see the [README](https://github.com/deephaven/deephaven-core#launch-python-with-example-data).  For full instructions to work with Deephaven, see the [Quick start](https://deephaven.io/core/docs/getting-started/quickstart/).

To launch Deephaven, execute the following in your deployment directory:

```bash
curl -O https://raw.githubusercontent.com/deephaven/deephaven-core/main/containers/python-examples/base/docker-compose.yml
docker compose pull
docker compose up -d
```
