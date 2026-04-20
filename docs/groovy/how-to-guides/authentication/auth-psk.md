---
title: Configure and use pre-shared key authentication
sidebar_label: Pre-shared key
---

This guide will show you how to configure and use pre-shared key (PSK) authentication for Deephaven. PSK is the default authentication method used by Deephaven, while [username/password authentication](./auth-uname-pw.md) is the most basic method of authentication available.

A pre-shared key is a shared secret between two or more parties that must be presented in order to be granted access to a particular resource. For Deephaven, the shared secret is a password. A typical example of a pre-shared key that guards a resource is a password to gain access to a Wi-Fi network. Anyone with the password can connect to the network, regardless of their affiliation with its owner.

## Default configuration

If you open Deephaven with pre-shared key enabled, you'll be greeted by this login screen:

![The Deephaven login screen](../../assets/tutorials/psk-loginscreen.png)

By default, Deephaven sets the pre-shared key as a series of randomly selected characters. This randomly generated key can be found in the Docker logs when you start the Deephaven container with `docker compose up`:

![Log readout containing a randomly generated login key](../../assets/tutorials/default-psk.png)

You can enter this key in the login screen or append it to the end of the Deephaven URL to gain access.

> [!NOTE]
> If you run Docker in detached mode (`docker compose up -d`), you will need to access the logs with the following command:
>
> `docker compose logs -f`
>
> To search the logs for the token:
>
> `docker compose logs -f | grep "access through pre-shared key"`

## Setting your own key

To set your own key, set the `authentication.psk` property by appending `-Dauthentication.psk=YOUR_SECRET_KEY` to the `START_OPTS` environment variable.

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

![Log readout showing the pre-shared key and address](../../assets/how-to/custom-psk2.png)

You can also store the key in an environment variable. This is recommended over plaintext.

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

This will use the value of your system's environment variable - `DEEPHAVEN_PSK` - as the key. This value can be set inline when you start Deephaven via Docker:

```bash
DEEPHAVEN_PSK=YOUR_PASSWORD_HERE docker compose up
```

Or alternatively:

```bash
export DEEPHAVEN_PSK=YOUR_PASSWORD_HERE
docker compose up
```

![Log readout showing the pre-shared key and web address](../../assets/how-to/custom-psk2.png)

You can also set environment variables in a `.env` file in the same directory as `docker-compose.yml`. See [Use an environment file](https://docs.docker.com/compose/environment-variables/env-file/) for more information.

## Related documentation

- [Install guide for Docker](../../getting-started/docker-install.md)
- [How to disable authentication](./auth-anon.md)
- [How to configure the Docker application](../configuration/docker-application.md)
