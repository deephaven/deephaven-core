---
title: Enable anonymous authentication
sidebar_label: Anonymous
---

This guide will show you how to disable authentication for Deephaven run from Docker. This is also known as anonymous authentication. **Anonymous authentication allows anyone to connect to a Deephaven instance if they can reach it**.

> [!WARNING]
> Anonymous authentication provides no application security.

## Deephaven run from Docker

### Docker compose

To enable anonymous authentication, the `AuthHandlers` property must be set to `io.deephaven.auth.AnonymousAuthenticationHandler` by appending `-DAuthHandlers=io.deephaven.auth.AnonymousAuthenticationHandler` to the `START_OPTS` environment variable:

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

The Docker logs will show the following message when anonymous authentication is enabled.

![The Docker logs display a warning when anonymous authentication is enabled](../../assets/how-to/anon-auth.png)

## Related documentation

- [Install guide for Docker](../../getting-started/docker-install.md)
- [How to configure pre-shared key authentication](./auth-psk.md)
- [How to configure the Docker application](../configuration/docker-application.md)
