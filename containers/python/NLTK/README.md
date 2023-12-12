# Overview

A Docker Compose deployment for [Deephaven](https://deephaven.io).

## Features

- [Deephaven](https://deephaven.io)
- [Python](https://python.org)
- [NLTK](https://www.nltk.org/)

## Launch Deephaven

For launch instructions, see the [README](https://github.com/deephaven/deephaven-core/#launch-python).  For full instructions to work with Deephaven, see the [Quick start](https://deephaven.io/core/docs/tutorials/quickstart).

To launch Deephaven, execute the following in your deployment directory:

```bash
curl -O https://raw.githubusercontent.com/deephaven/deephaven-core/main/containers/python/NLTK/docker-compose.yml
docker compose pull
docker compose up -d
```
