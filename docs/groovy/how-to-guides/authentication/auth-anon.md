---
title: Enable anonymous authentication
---

This guide shows how to enable anonymous authentication, which replaces [pre-shared key authentication](./auth-psk.md) for Deephaven run from Docker. Anonymous authentication lets anyone who can reach a Deephaven instance connect to it.

> [!WARNING]
> Anonymous authentication provides no application security.

## Configuration

To enable anonymous authentication, set the `AuthHandlers` JVM system property. Pass it as `-DAuthHandlers=io.deephaven.auth.AnonymousAuthenticationHandler`. `AuthHandlers` is a comma-separated list of the authentication handler classes the server accepts. Its default is the pre-shared key handler.

Setting `AuthHandlers` to only the anonymous handler replaces pre-shared key authentication, so clients can connect without credentials. To accept both, list both handlers, separated by a comma.

## Docker Compose

Set the `START_OPTS` environment variable in the service's `environment` list, as shown below. `START_OPTS` holds additional JVM options that the server appends to its defaults.

This example uses the `server-slim` image, which runs the Groovy console.

```yaml
services:
  deephaven:
    image: ghcr.io/deephaven/server-slim:latest
    ports:
      - "${DEEPHAVEN_PORT:-10000}:10000"
    volumes:
      - ./data:/data
    environment:
      - START_OPTS=-Xmx4g -DAuthHandlers=io.deephaven.auth.AnonymousAuthenticationHandler
```

The Docker logs show the following message when you enable anonymous authentication. The `authentication.anonymous.warn` property controls this warning, which is on by default.

![The Docker logs display a warning when anonymous authentication is enabled](../../assets/how-to/anon-auth.png)

## Related documentation

- [Install guide for Docker](../../getting-started/docker-install.md)
- [How to configure pre-shared key authentication](./auth-psk.md)
- [How to configure the Docker application](../configuration/docker-application.md)
