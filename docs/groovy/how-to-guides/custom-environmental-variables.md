---
title: Set custom environmental variables
sidebar_label: Custom environmental variables
---

# Environmental variables

Environmental variables are commonly found amongst applications, especially when dealing with sensitive information like API keys. This guide shows how to set environmental variables for use in Deephaven via Docker.

## docker-compose.yml

Environmental variables must be set in the `server` service of the `docker-compose.yml` file. The default Deephaven `docker-compose.yml` file already has an `environment` section, so you'll need to expand on this.

```
services:
  server:
    image: ghcr.io/deephaven/server:${VERSION:-latest}
    expose:
      - '8080'
    volumes:
      - ./data:/data
      - api-cache:/cache
    environment:
      - JAVA_TOOL_OPTIONS=-Xmx4g -Ddeephaven.console.type=python
      - MY_VALUE=test
```

You can now see your environmental variables in Deephaven on launch:

```groovy
def env = System.getenv()

env.each{
  println it
}
```

## Parameterizing the environmental variables

The above example hardcodes the value of `MY_VALUE` in the `docker-compose.yml` file. This may not be ideal for sensitive information, or for easily doing deployments in different environments.

Instead, we can use the bracket syntax `${MY_VALUE}` within our `docker-compose.yml` file. The value in the bracket will be filled in by the current environmental variable on the system.

```
- MY_VALUE=${MY_VALUE}
```

Now the value of `MY_VALUE` in the Deephaven server will match the value of `MY_VALUE` on the local machine.

## Settings environmental variables locally

There are many ways to set environmental variables locally. Covering all of them is beyond the scope of this documentation, but there are many guides out there for that information. Here we will show two primary ways to set environmental variables for use within Deephaven.

### .env file

In the same directory as your `docker-compose.yml` file, you can create a `.env` that contains key-value pairs to assign your environmental variables.

`.env`

```
MY_VALUE=test
```

These environmental variables will be set when using `docker compose up`.

### CLI parameters

You can also include environmental variables when calling `docker compose up`. Simply assign the key-value pairs before the `docker compose up` command when executing it.

```
MY_VALUE=test docker compose up
```

## Related documentation

- [Environment variables in Compose](https://docs.docker.com/compose/environment-variables/)
- [Declare default environment variables in file](https://docs.docker.com/compose/env-file/)
