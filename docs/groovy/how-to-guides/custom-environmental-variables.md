---
title: Set custom environment variables
sidebar_label: Custom environment variables
---

Applications commonly use environment variables, especially for sensitive information like API keys. This guide shows how to set environment variables for use in Deephaven via Docker.

## docker-compose.yml

Environment variables must be set in the `deephaven` service of the `docker-compose.yml` file. The default Deephaven `docker-compose.yml` file already has an `environment` section, so you'll need to expand on this.

```yaml
services:
  deephaven:
    image: ghcr.io/deephaven/server-slim:${VERSION:-latest}
    ports:
      - "${DEEPHAVEN_PORT:-10000}:10000"
    volumes:
      - ./data:/data
    environment:
      - START_OPTS=-Xmx4g
      - MY_VALUE=test
```

You can access your environment variables in Deephaven using `System.getenv`:

```groovy
def myValue = System.getenv("MY_VALUE")
println myValue
```

> [!WARNING]
> Avoid calling `System.getenv()` with no arguments and printing the result. It dumps all environment variables, including sensitive values like API keys and passwords, which can expose secrets in logs or shared notebooks.

## Parameterizing the environment variables

The above example hardcodes the value of `MY_VALUE` in the `docker-compose.yml` file. This may not be ideal for sensitive information, or for easily doing deployments in different environments.

Instead, you can use the variable substitution syntax `${MY_VALUE}` within your `docker-compose.yml` file. Docker Compose fills in the value with the matching environment variable from the host system.

```yaml
- MY_VALUE=${MY_VALUE}
```

Now the value of `MY_VALUE` in the Deephaven server matches the value of `MY_VALUE` on the local machine.

## Setting environment variables locally

There are many ways to set environment variables locally. Covering all of them is beyond the scope of this documentation, but many guides are available for that information. Here we show two primary ways to set environment variables for use within Deephaven.

### .env file

In the same directory as your `docker-compose.yml` file, you can create a `.env` that contains key-value pairs to assign your environment variables.

`.env`

```
MY_VALUE=test
```

These environment variables are set when you run `docker compose up`.

### CLI parameters

You can also include environment variables when calling `docker compose up`. Simply assign the key-value pairs before the `docker compose up` command.

```shell
MY_VALUE=test docker compose up
```

## Related documentation

- [Environment variables in Compose](https://docs.docker.com/compose/environment-variables/)
- [Declare default environment variables in file](https://docs.docker.com/compose/env-file/)
