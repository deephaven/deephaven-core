---
title: Enable anonymous authentication
---

This guide shows how to enable anonymous authentication, which replaces [pre-shared key authentication](./auth-psk.md) for Deephaven. It covers Docker and pip-installed Deephaven. Anonymous authentication lets anyone who can reach a Deephaven instance connect to it.

> [!WARNING]
> Anonymous authentication provides no application security.

## Configuration

To enable anonymous authentication, set the `AuthHandlers` JVM system property. Pass it as `-DAuthHandlers=io.deephaven.auth.AnonymousAuthenticationHandler`. `AuthHandlers` is a comma-separated list of the authentication handler classes the server accepts. Its default is the pre-shared key handler.

Setting `AuthHandlers` to only the anonymous handler replaces pre-shared key authentication, so clients can connect without credentials. To accept both, list both handlers, separated by a comma. Follow the setup steps for your environment:

- Docker: see [Docker Compose](#docker-compose).
- pip: see [pip-installed Deephaven](#pip-installed-deephaven).

## Docker Compose

Set the `START_OPTS` environment variable in the service's `environment` list, as shown below. `START_OPTS` holds additional JVM options that the server appends to its defaults.

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

The Docker logs show the following message when you enable anonymous authentication.

![The Docker logs display a warning when anonymous authentication is enabled](../../assets/how-to/anon-auth.png)

## pip-installed Deephaven

The following Python code starts a server on port 10000, with anonymous authentication enabled. The property goes in the `jvm_args` parameter. If you don't set the `host` parameter or the `http.host` property, the server binds to all network interfaces, so other machines can reach it without credentials. This example sets `host="localhost"` so that only the local machine can connect. When you connect to the IDE in your web browser, it doesn't prompt for a password.

```python skip-test
from deephaven_server import Server

s = Server(
    host="localhost",
    jvm_args=["-DAuthHandlers=io.deephaven.auth.AnonymousAuthenticationHandler"],
)
s.start()
```

The server logs a warning at startup that anonymous authentication is enabled. The `authentication.anonymous.warn` property controls this warning, which is on by default.

## Related documentation

- [Install guide for Docker](../../getting-started/docker-install.md)
- [Install and run Deephaven with pip](../../getting-started/pip-install.md)
- [How to configure pre-shared key authentication](./auth-psk.md)
- [How to configure the Docker application](../configuration/docker-application.md)
- [Pydoc](/core/pydoc/code/deephaven_server.html#deephaven_server.Server)
