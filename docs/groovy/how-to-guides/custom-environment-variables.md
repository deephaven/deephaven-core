---
title: Set custom environment variables
---

Applications commonly use environment variables, especially for sensitive information like API keys. This guide shows how to set environment variables for use in Deephaven when you run it with Docker Compose.

If you run Deephaven without Docker, the server already sees the environment variables of the process that starts it, usually your shell. That covers the [production application](../getting-started/production-application.md) and a [source build](../getting-started/launch-build.md). Skip to [Read environment variables in Deephaven](#read-environment-variables-in-deephaven).

## Add variables to the Docker Compose file

To make an environment variable available to Deephaven, set it in the `environment` section of the `deephaven` service in your `docker-compose.yml` file. The [example `docker-compose.yml` file in Install and run with Docker](../getting-started/docker-install.md#modify-the-deployment) already has an `environment` section, so add your variables to it. The examples below use that file. If you use a custom image, keep its name in the `image:` line.

The example file's [`START_OPTS`](./configuration/docker-application.md#set-jvm-options) entry sets JVM options. To learn what `START_OPTS` and the rest of the example file configure, see [Configure the Deephaven Docker application](./configuration/docker-application.md). Keep `START_OPTS`, and add each of your own variables as a separate entry, like `MY_VALUE` in this example:

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

## Parameterize the variables

The `MY_VALUE=test` entry in [Add variables to the Docker Compose file](#add-variables-to-the-docker-compose-file) hardcodes the value in the `docker-compose.yml` file. Hardcoding a secret, such as an API key, exposes it to anyone who can read the file, and hardcoded values make the file harder to reuse across environments.

Instead, you can use the variable substitution syntax `${MY_VALUE}` within your `docker-compose.yml` file. Docker Compose calls this [interpolation](https://docs.docker.com/compose/how-tos/environment-variables/variable-interpolation/). The example file already uses this syntax for `VERSION` and `DEEPHAVEN_PORT`. Replace the hardcoded value in the `environment` section with a substitution:

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
      - MY_VALUE=${MY_VALUE}
```

When you run `docker compose up`, Docker Compose replaces `${MY_VALUE}` with the value of `MY_VALUE` from your shell or from a [`.env` file](#use-a-env-file), and the Deephaven server sees that value. If `MY_VALUE` isn't set locally:

- By default, Docker Compose prints a warning and sets `MY_VALUE` to an empty string in the container.
- With the `:-` form, Docker Compose uses a fallback instead. For example, `MY_VALUE=${MY_VALUE:-default}` sets `MY_VALUE` to `default` when it's unset or empty locally. The example file uses the same form in `${VERSION:-latest}`.

Substitution keeps the value out of the file, but anyone who can run `docker inspect` on the container can still see it. For sensitive values, consider [secrets in Docker Compose](https://docs.docker.com/compose/how-tos/use-secrets/).

## Set the variables locally

There are many ways to set environment variables locally. This section shows two common ones for Docker Compose.

Docker Compose doesn't [pass your shell variables or your `.env` file into the container](https://docs.docker.com/compose/how-tos/environment-variables/set-environment-variables/) on its own. The `environment` section has to name each variable, either with a `${...}` reference, like the `MY_VALUE=${MY_VALUE}` entry from [Parameterize the variables](#parameterize-the-variables), or as a name-only entry, such as `- MY_VALUE`. Both methods below rely on the `MY_VALUE=${MY_VALUE}` entry.

### Use a `.env` file

In the same directory as your `docker-compose.yml` file, create a `.env` file that contains key-value pairs:

```text title=".env"
MY_VALUE=test
```

When you run `docker compose up`, Docker Compose reads `.env` and substitutes its values into `${...}` references in `docker-compose.yml`, so the `MY_VALUE=${MY_VALUE}` entry passes `test` into the Deephaven container.

### Set variables on the command line

You can also include environment variables when calling `docker compose up`. Assign the key-value pairs before the command:

```shell
MY_VALUE=test docker compose up
```

A value set this way takes precedence over the same variable in `.env`.

## Read environment variables in Deephaven

Your Groovy code in Deephaven reads environment variables the same way whether you run the server with [Docker Compose](../getting-started/docker-install.md), the [production application](../getting-started/production-application.md), or a [source build](../getting-started/launch-build.md). Use `System.getenv`:

```groovy order=:log
def myValue = System.getenv("MY_VALUE")
println myValue
```

The docs build doesn't set `MY_VALUE`, so the output above is `null`. If `MY_VALUE` isn't set in the server's environment, `System.getenv` returns `null`. If the Compose file passes `MY_VALUE=${MY_VALUE}` but `MY_VALUE` isn't set locally, the variable exists but is empty, so `System.getenv` returns `""`.

> [!WARNING]
> Avoid calling `System.getenv` with no arguments and printing the result. It dumps all environment variables, including sensitive values like API keys and passwords, which can expose secrets in logs or shared notebooks.

## Related documentation

- [Install and run with Docker](../getting-started/docker-install.md)
- [Install and run the Deephaven production application](../getting-started/production-application.md)
- [Build and run Deephaven from source code](../getting-started/launch-build.md)
- [Configure the Deephaven Docker application](./configuration/docker-application.md)
- [Environment variables in Compose](https://docs.docker.com/compose/how-tos/environment-variables/)
- [Set, use, and manage variables in a Compose file with interpolation](https://docs.docker.com/compose/how-tos/environment-variables/variable-interpolation/)
