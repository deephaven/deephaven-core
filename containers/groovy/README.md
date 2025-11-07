
# Overview

A Docker Compose deployment for [Deephaven](https://deephaven.io).

## Features

- [Deephaven](https://deephaven.io)
- [Groovy](https://groovy-lang.org/) scripting

## Launch Deephaven

For launch instructions, see the [README](https://github.com/deephaven/deephaven-core/#launch-groovy--java).  For full instructions to work with Deephaven, see the [Quick start](https://deephaven.io/core/groovy/docs/tutorials/quickstart).

To launch Deephaven, execute the following in your deployment directory:

```bash
curl -O https://raw.githubusercontent.com/deephaven/deephaven-core/main/containers/groovy/docker-compose.yml
docker compose pull
docker compose up -d
```
