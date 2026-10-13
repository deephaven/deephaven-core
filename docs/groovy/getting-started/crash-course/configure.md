---
title: Configure your Deephaven Instance
---

This section covers configuration details needed to take your Deephaven instance beyond the defaults.

## Authentication

Keeping your real-time data secure is one of Deephaven's top priorities. As such, the default installation of Deephaven is equipped with [pre-shared key (PSK) authentication](../../how-to-guides/authentication/auth-psk.md). By default, this key is randomly generated and available in the Docker logs (if using Docker) on startup:

![Docker logs showing the PSK](../../assets/how-to/docker-logs.png)

It is advised that you change the password from a randomly generated string to your own password for enhanced security. For the [Docker one-liner installation](../../getting-started/docker-install.md), you can set your password with the `-Dauthentication.psk` flag:

```bash skip-test
docker run --rm --name deephaven -p 10000:10000 -v "$(pwd)/data:/data" --env START_OPTS=-Dauthentication.psk=YOUR_PASSWORD_HERE ghcr.io/deephaven/server-slim:latest
```

To learn more about PSK authentication, see the [PSK authentication guide](../../how-to-guides/authentication/auth-psk.md).

In addition to PSK authentication, Deephaven supports the following types of authentication:

- [Anonymous](../../how-to-guides/authentication/auth-anon.md)
- [Keycloak](../../how-to-guides/authentication/auth-keycloak.md)
- [mTLS](../../how-to-guides/authentication/auth-mtls.md)
- [Username / Password](../../how-to-guides/authentication/auth-uname-pw.md)

## Deployments

Deephaven publishes several Docker images. For Groovy, use `server-slim`, which starts a Groovy console. The other images, such as `server` and `server-all-ai`, start a Python console and come pre-installed with different Python libraries. Each image is available in several [versions](../../getting-started/docker-install.md#image-versions), such as `latest` or a specific release. You also have the option of using [Deephaven's example data](https://github.com/deephaven/examples) with your deployment.

To run the latest `server-slim` image, use this Docker command:

```bash skip-test
docker run --rm --name deephaven -p 10000:10000 ghcr.io/deephaven/server-slim:latest
```

To learn more about deployments, check out the guide for [installing Deephaven with Docker](../../getting-started/docker-install.md).

## Installing Java packages

Even with a standard deployment, you may need to install new Java packages at some point. To make a Java package available, add its JAR to Deephaven's classpath. With Docker, you can either mount a directory of JARs to `/apps/libs` or build a custom image that includes them. A custom image keeps the package available in every container started from it. See the [user guide on installing Java packages](../../how-to-guides/install-and-use-java-packages.md) for more information.

## RAM

Large datasets require significant memory. Unless you set `-Xmx`, the JVM picks a maximum heap size based on the memory available, which is often too small for large data. Fortunately, it's easy to give Deephaven more memory.

If you're using Docker-installed Deephaven on Docker Desktop, Docker Desktop limits the memory available to containers. You can raise this limit in `Settings > Resources` (see the [Docker Desktop settings](https://docs.docker.com/desktop/settings-and-maintenance/settings/#resources)). Then, set the maximum Java heap size for Deephaven with the `-Xmx` flag. Here's the command to run the Deephaven server with a 16 GB maximum heap:

```bash skip-test
docker run --rm --name deephaven -p 10000:10000 --env START_OPTS=-Xmx16g ghcr.io/deephaven/server-slim:latest
```

The [memory guide](../../how-to-guides/heap-size.md) explains more about allocating memory in Deephaven.

## Keep your instance up-to-date

The data world is ever-evolving, and so is Deephaven. It's easy to keep your instance up-to-date, taking advantage of the latest features and bug fixes.

If you're running Deephaven with Docker, use `docker pull` to download the latest image, then start a new container from it:

```bash
docker pull ghcr.io/deephaven/server-slim:latest
```

Users with custom Docker installations should see [updating Deephaven](../../how-to-guides/configuration/updating-deephaven.md#update-deephaven) for information on updating custom instances.
