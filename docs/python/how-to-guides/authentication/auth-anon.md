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
    image: ghcr.io/deephaven/server:latest
    ports:
      - "${DEEPHAVEN_PORT:-10000}:10000"
    volumes:
      - ./data:/data
    environment:
      - START_OPTS=-Xmx4g -DAuthHandlers=io.deephaven.auth.AnonymousAuthenticationHandler
```

The Docker logs will show the following message when anonymous authentication is enabled.

![The Docker logs display a warning when anonymous authentication is enabled](../../assets/how-to/anon-auth.png)

## pip-installed Deephaven

Anonymous authentication can be used with a Deephaven server run from Python by specifying an additional JVM argument in the server constructor. The following Python code starts a server on port 10000 with anonymous authentication enabled. If you connect to the IDE via your web browser, you will not be prompted for a password.

```python skip-test
from deephaven_server import Server

s = Server(
    jvm_args=["-DAuthHandlers=io.deephaven.auth.AnonymousAuthenticationHandler"]
).start()
```

## Related documentation

- [Install guide for Docker](../../getting-started/docker-install.md)
- [How to configure pre-shared key authentication](./auth-psk.md)
- [How to configure the Docker application](../configuration/docker-application.md)
