---
title: Configure and use pre-shared key authentication
---

This guide shows you how to configure and use pre-shared key (PSK) authentication for Deephaven. PSK is the default authentication method used by Deephaven. For per-user credentials, see [username/password authentication](./auth-uname-pw.md).

A pre-shared key is a shared secret between two or more parties. Anyone who presents the key gains access to the Deephaven server. For Deephaven, the shared secret is the pre-shared key.

In Deephaven, anyone who presents the key is logged in as a superuser. Share the key only with people who should have full access.

The key is the configuration property `authentication.psk`. The sections below show how to set it with the Java Virtual Machine (JVM) system property `-Dauthentication.psk=<key>` for each method.

## Deephaven run from Docker

### Default configuration

When you open Deephaven with its default authentication, you see this login screen:

![The Deephaven login screen](../../assets/tutorials/psk-loginscreen.png)

The login screen asks for a token. Enter your pre-shared key in that field.

By default, Deephaven sets the pre-shared key as a series of randomly selected characters. The Docker logs show this randomly generated key when you start the Deephaven container with `docker compose up`:

![Log readout containing a randomly generated login key](../../assets/tutorials/default-psk.png)

Enter this key in the login screen. Alternatively, open the URL printed in the log. That URL passes the key as the `psk` query parameter, for example `http://localhost:10000/?psk=<key>`.

The URL contains the key, so it can end up in browser history and in proxy or server logs. Treat it as a secret.

> [!NOTE]
> If you run Docker in detached mode (`docker compose up -d`), access the logs with the following command:
>
> `docker compose logs -f`
>
> To search the logs for the key:
>
> `docker compose logs -f | grep "access through pre-shared key"`

### Setting your own key

To set your own key, set the `authentication.psk` property by appending `-Dauthentication.psk=YOUR_PASSWORD_HERE` to the `START_OPTS` environment variable. `START_OPTS` holds extra JVM options that the Deephaven Docker image appends to its defaults. See [How to configure the Docker application](../configuration/docker-application.md) for details.

The example file below sets the key to `YOUR_PASSWORD_HERE`.

```yaml
services:
  deephaven:
    image: ghcr.io/deephaven/server-slim:latest
    ports:
      - "${DEEPHAVEN_PORT:-10000}:10000"
    volumes:
      - ./data:/data
    environment:
      - START_OPTS=-Xmx4g -Dauthentication.psk=YOUR_PASSWORD_HERE
```

Start Deephaven with `docker compose up`. The log shows your key and the login address:

![Log readout showing the pre-shared key and address](../../assets/how-to/custom-psk2.png)

The server writes the key to its startup log, so limit who can read the container logs.

To keep the key out of `docker-compose.yml` so it isn't committed to source control, read it from an environment variable instead:

```yaml
services:
  deephaven:
    image: ghcr.io/deephaven/server-slim:latest
    ports:
      - "${DEEPHAVEN_PORT:-10000}:10000"
    volumes:
      - ./data:/data
    environment:
      - START_OPTS=-Xmx4g -Dauthentication.psk=${DEEPHAVEN_PSK}
```

Compose substitutes the value of the `DEEPHAVEN_PSK` environment variable into `START_OPTS`. Set the variable inline when you start Deephaven:

```bash
DEEPHAVEN_PSK=YOUR_PASSWORD_HERE docker compose up
```

Alternatively, export the variable first:

```bash
export DEEPHAVEN_PSK=YOUR_PASSWORD_HERE
docker compose up
```

You can also set environment variables in a `.env` file in the same directory as `docker-compose.yml`. See [Use an environment file](https://docs.docker.com/compose/environment-variables/env-file/) for more information.

If `DEEPHAVEN_PSK` is unset or empty, the server ignores the empty value and generates a random key. Check the startup log for the key that is in use.

## Connect from Java or Groovy

A Java or Groovy client passes the key in its session configuration. Set `authenticationTypeAndValue` on the `SessionConfig` builder to the handler class name, a space, and the key: `io.deephaven.authentication.psk.PskAuthenticationHandler <key>`. For a complete example of connecting a Java client, see the [Java client guide](../java-client.md).

## Other ways to set the key

Besides the Docker method above, set the key in these ways:

- For the production application, add `-Dauthentication.psk=YOUR_PASSWORD_HERE` to `START_OPTS` when you run the `start` script. See the [production application guide](../../getting-started/production-application.md).
- For a source build, pass `-Ppsk=YOUR_PASSWORD_HERE` to the Gradle run task: `./gradlew server-jetty-app:run -Pgroovy -Ppsk=YOUR_PASSWORD_HERE`. See the [source build guide](../../getting-started/launch-build.md).

## Log in from the JS API

Applications that use the [Deephaven JS API](/core/client-api/javascript/modules/dh.html) log in with the pre-shared key. Call [`login`](/core/client-api/javascript/classes/dh.CoreClient.html#login) with `type` set to the pre-shared key handler's class name and `token` set to the key. [Use the JS API](../use-jsapi.md) shows how to load the API into a web page.

```javascript
import dh from "http://localhost:10000/jsapi/dh-core.js";

async function main() {
  const client = new dh.CoreClient("http://localhost:10000");
  await client.login({
    type: "io.deephaven.authentication.psk.PskAuthenticationHandler",
    token: "<your key>",
  });
}

main().catch((err) => console.error("Login failed:", err));
```

Replace `http://localhost:10000` with the address of your server, in both the import and the client, and `<your key>` with the key.

## Related documentation

- [Install guide for Docker](../../getting-started/docker-install.md)
- [Production application guide](../../getting-started/production-application.md)
- [Source build guide](../../getting-started/launch-build.md)
- [How to disable authentication](./auth-anon.md)
- [How to configure the Docker application](../configuration/docker-application.md)
- [Username/password authentication](./auth-uname-pw.md)
