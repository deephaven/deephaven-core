---
title: Enable anonymous authentication
---

This guide shows how to enable anonymous authentication, which replaces [pre-shared key authentication](./auth-psk.md) for Deephaven. It covers Docker, pip, the production application, and builds from source. Anonymous authentication lets anyone who can reach a Deephaven instance connect to it.

> [!WARNING]
> Anonymous authentication provides no application security.

## Configuration

To enable anonymous authentication, set the `AuthHandlers` property to `io.deephaven.auth.AnonymousAuthenticationHandler`. Pass it as a JVM system property, `-DAuthHandlers=io.deephaven.auth.AnonymousAuthenticationHandler`, or set it in the server's [configuration file](../configuration/config-file.md). `AuthHandlers` is a comma-separated list of the authentication handler classes the server accepts. Its default is the pre-shared key handler.

Setting `AuthHandlers` to only the anonymous handler replaces pre-shared key authentication, so clients can connect without credentials. To accept both, list both handlers, separated by a comma. Follow the setup steps for your environment:

- Docker: see [Docker Compose](#docker-compose).
- pip: see [pip-installed Deephaven](#pip-installed-deephaven).
- Production application: add the property to `START_OPTS` when you run the `start` script. See [Authentication](../../getting-started/production-application.md#authentication) in the production application guide.
- Build from source: run `./gradlew server-jetty-app:run -Panonymous`. The `anonymous` Gradle property sets `AuthHandlers` to the anonymous handler.

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

The following Python code starts a server on the default port, 10000, with anonymous authentication enabled. The property goes in the `jvm_args` parameter. If you don't set the `host` parameter or the `http.host` property, the server binds to all network interfaces, so other machines can reach it without credentials. This example sets `host="localhost"` so that only the local machine can connect. When you connect to the IDE in your web browser, it doesn't prompt for a password.

```python skip-test
from deephaven_server import Server

s = Server(
    host="localhost",
    jvm_args=["-DAuthHandlers=io.deephaven.auth.AnonymousAuthenticationHandler"],
)
s.start()
```

The server logs a warning at startup that anonymous authentication is enabled. The `authentication.anonymous.warn` property controls this warning, which is on by default.

## Log in from the JS API

Applications that use the [Deephaven JS API](/core/client-api/javascript/modules/dh.html) log in anonymously. Call [`login`](/core/client-api/javascript/classes/dh.CoreClient.html#login) with `type` set to `dh.CoreClient.LOGIN_TYPE_ANONYMOUS`. No token is needed. [Use the JS API](../use-jsapi.md) shows how to load the API into a web page.

```javascript
import dh from "http://localhost:10000/jsapi/dh-core.js";

async function main() {
  const client = new dh.CoreClient("http://localhost:10000");
  await client.login({
    type: dh.CoreClient.LOGIN_TYPE_ANONYMOUS,
  });
}

main().catch((err) => console.error("Login failed:", err));
```

Replace `http://localhost:10000` with the address of your server, in both the import and the client.

## Related documentation

- [Install guide for Docker](../../getting-started/docker-install.md)
- [Install and run Deephaven with pip](../../getting-started/pip-install.md)
- [Production application](../../getting-started/production-application.md)
- [Build from source](../../getting-started/launch-build.md)
- [Deephaven configuration files](../configuration/config-file.md)
- [How to configure pre-shared key authentication](./auth-psk.md)
- [How to configure the Docker application](../configuration/docker-application.md)
- [The `deephaven-server` package on PyPi](https://pypi.org/project/deephaven-server/)
