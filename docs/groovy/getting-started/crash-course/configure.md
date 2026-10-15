---
title: Configure your Deephaven instance
---

This page shows how to take your Deephaven instance beyond the defaults: set up authentication, learn which images and example data are available, add Java packages, give it more memory, and keep it up to date. The Docker examples build on the command you ran in [Get started](./get-started.md).

## Authentication

Keeping your real-time data secure is one of Deephaven's top priorities. By default, Deephaven uses [pre-shared key (PSK) authentication](../../how-to-guides/authentication/auth-psk.md). In [Get started](./get-started.md), you set the key with the `-Dauthentication.psk` flag in the `docker run` command.

- With the flag, your key stays the same across server restarts.
- Without the flag, Deephaven generates a new random key each time the server starts.

For enhanced security, replace the placeholder `YOUR_PASSWORD_HERE` in the `-Dauthentication.psk` flag with your own long, hard-to-guess key:

```bash skip-test
docker run --rm --name deephaven -p 10000:10000 -v "$(pwd)/data:/data" --env START_OPTS=-Dauthentication.psk=YOUR_PASSWORD_HERE ghcr.io/deephaven/server-slim:latest
```

In both cases, Deephaven prints the key in the server logs on startup, so you can find it there if you forget it. With the Get started command, the logs print in the terminal where you ran `docker run`. The key appears in a line like this one from a Docker Compose log:

![Docker Compose logs showing the PSK](../../assets/how-to/docker-logs.png)

To learn more about PSK authentication, see the [PSK authentication guide](../../how-to-guides/authentication/auth-psk.md).

In addition to PSK authentication, Deephaven supports the following types of authentication:

- [Anonymous](../../how-to-guides/authentication/auth-anon.md)
- [Keycloak](../../how-to-guides/authentication/auth-keycloak.md)
- [mTLS](../../how-to-guides/authentication/auth-mtls.md)
- [Username/password](../../how-to-guides/authentication/auth-uname-pw.md)

## Images and example data

Deephaven provides [multiple Docker images](../../getting-started/docker-install.md#choose-a-deployment), called deployments. Each image is available in several [versions](../../getting-started/docker-install.md#image-versions), such as `latest` or a specific release. For Groovy, use `server-slim`, which starts a Groovy console. The other images, such as `server` and `server-all-ai`, start a Python console and come pre-installed with different Python libraries.

Your [Get started](./get-started.md) command already runs `ghcr.io/deephaven/server-slim:latest`, the latest `server-slim` image.

You can also run Deephaven with [Deephaven's example data](https://github.com/deephaven/examples), [Redpanda](https://redpanda.com/), or both. Deephaven publishes pre-built `docker-compose.yml` files that run `server-slim` this way. To pick one and start it, see [Choose a deployment](../../getting-started/docker-install.md#choose-a-deployment).

To learn more about deployments, check out the guide for [installing Deephaven with Docker](../../getting-started/docker-install.md).

## Install Java packages

At some point, you may need a Java package that `server-slim` doesn't include. To make a Java package available, add its JAR to Deephaven's classpath. With Docker, you can either mount a directory of JARs to `/apps/libs` or build a custom image that includes them. A custom image keeps the package available in every container started from it. See the [user guide on installing Java packages](../../how-to-guides/install-and-use-java-packages.md) for more information.

## Give Deephaven more memory

Large datasets require significant memory. Deephaven runs on the Java Virtual Machine (JVM), and the JVM's maximum heap size caps how much memory Deephaven can use for data. You set it with the `-Xmx` flag. If you don't, the JVM picks a size based on the memory available, which is often too small for large data.

Fortunately, it's easy to give Deephaven more memory:

1. On Docker Desktop, raise the memory limit under **Settings > Resources > Advanced**, as described in the [Docker Desktop settings documentation](https://docs.docker.com/desktop/settings-and-maintenance/settings/#resources). Containers can use only as much memory as Docker Desktop allows, which by default is half of your machine's memory.
2. Add the `-Xmx` flag to the `START_OPTS` environment variable you used in [Get started](./get-started.md). Separate multiple JVM options with spaces. The new `START_OPTS` value replaces the old one, so include your pre-shared key in it too.

Here's the Get started command with a 16 GB maximum heap added:

```bash skip-test
docker run --rm --name deephaven -p 10000:10000 -v "$(pwd)/data:/data" --env START_OPTS="-Xmx16g -Dauthentication.psk=YOUR_PASSWORD_HERE" ghcr.io/deephaven/server-slim:latest
```

The [memory guide](../../how-to-guides/heap-size.md) explains more about allocating memory in Deephaven.

## Keep your instance up to date

The data world is ever-evolving, and so is Deephaven. It's easy to keep your instance up to date and get the latest features and bug fixes.

To update, pull the latest image:

```bash
docker pull ghcr.io/deephaven/server-slim:latest
```

Then stop your running container and start a new one with the same `docker run` command you used in [Get started](./get-started.md). To stop the container, press <kbd>Ctrl+C</kbd> in its terminal or run `docker stop deephaven`. Because the Get started command mounts `./data`, your saved files carry over.

If you run Deephaven from a `docker-compose.yml` file or your own `Dockerfile`, see the [guide to updating Deephaven](../../how-to-guides/configuration/updating-deephaven.md#update-deephaven) to set the image version there.
