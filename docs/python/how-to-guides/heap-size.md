---
title: Configure and adjust heap size
---

This guide shows how to set the maximum heap size for the Java Virtual Machine (JVM) that runs the Deephaven server, and how to check and limit the memory that Docker containers can use.

The JVM heap is the memory that the JVM uses for objects, including the data of in-memory tables. The JVM option `-Xmx` sets the maximum heap size. For example, `-Xmx8g` sets the maximum to 8 GiB.

Where you set `-Xmx` depends on how you installed Deephaven:

- [Pre-built images](../getting-started/docker-install.md) run in [Docker](https://www.docker.com/) containers. Set the heap size with the `START_OPTS` environment variable.
- [Source builds](../getting-started/launch-build.md) start the server with Gradle. Set the heap size with `START_OPTS` or a Gradle property.
- [pip installs](../getting-started/pip-install.md) run the server with the `deephaven server` command or from Python. Set the heap size with `--jvm-args` on the command, or with `jvm_args` in Python.

The sections on container limits, the Docker Desktop memory cap, and Docker container memory apply only to the [pre-built Docker image](../getting-started/docker-install.md).

## Set the heap size

If you set a container memory limit (see [Limit container resources](#limit-container-resources)), keep `-Xmx` below that limit. The JVM also uses memory outside the heap. The `-Xmx8g` examples on this page assume a limit above 8 GiB, so they do not fit the `6000M` limit in that section.

### Pre-built images

Set `START_OPTS` in the environment of the service that runs the Deephaven server image. The [example Compose files](../getting-started/docker-install.md#modify-the-deployment) name that service `deephaven`:

```yaml
services:
  deephaven:
    environment:
      - START_OPTS=-Xmx8g
```

Then start Deephaven again, as described in [Start the application](../getting-started/docker-install.md#start-the-application).

The example Compose files set `START_OPTS=-Xmx4g`. The published server image does not set a heap size. If you do not set `-Xmx`, the JVM uses its default maximum heap, which is 25% of the memory the JVM can use: the container's limit if you set one, or the Docker host's memory if you do not.

To set the heap size without [Docker Compose](https://docs.docker.com/compose/), add `--env START_OPTS=-Xmx8g` to the `docker create` command, then start the container:

```bash
docker create --name deephaven -p 10000:10000 --env START_OPTS=-Xmx8g ghcr.io/deephaven/server:latest
docker start deephaven
```

If a container named `deephaven` already exists, remove it first with `docker rm -f deephaven`, as described in [Modify the deployment](../getting-started/docker-install.md#modify-the-deployment), or pass a different `--name`.

### Source builds

Set `START_OPTS` in the shell where you start the server. The Gradle run task reads it:

```bash
START_OPTS="-Xmx8g" ./gradlew server-jetty-app:run
```

You can also pass the option as a Gradle property:

```bash
./gradlew server-jetty-app:run -Pdeephaven.startOpts="-Xmx8g"
```

### Pip installs

If you installed Deephaven with [pip](../getting-started/pip-install.md), pass the option as a JVM argument. Use `--jvm-args` with the `deephaven server` command:

```bash
deephaven server --port 10000 --jvm-args "-Xmx8g"
```

Or pass `jvm_args` to the `Server` constructor:

```python skip-test
from deephaven_server import Server

s = Server(port=10000, jvm_args=["-Xmx8g"])
s.start()
```

## Limit container resources

This section applies to the [pre-built Docker image](../getting-started/docker-install.md).

To limit a container's memory and CPU, set `deploy.resources` for its service in your [Docker Compose](https://docs.docker.com/compose/) YAML file:

```yaml
deploy:
  resources:
    limits:
      cpus: "4.0"
      memory: 6000M
    reservations:
      memory: 1000M
```

If you do not need the `deploy` block, you can set the same limits directly on the service with `mem_limit`, `mem_reservation`, and `cpus`:

```yaml
mem_limit: 6000m
mem_reservation: 1000M
cpus: 4.0
```

Set `-Xmx` below the memory limit. The JVM also uses memory outside the heap, such as thread stacks and metaspace, which stores class metadata. That memory counts against the limit. For example, use `-Xmx4g` with a `6000M` limit.

## Docker Desktop memory cap

This section applies to the [pre-built Docker image](../getting-started/docker-install.md).

[Docker Desktop](https://docs.docker.com/desktop/) also caps the memory that its containers can use. Keep the container limits you set in [Limit container resources](#limit-container-resources) below this cap.

To change the cap, open Docker Desktop's settings and adjust **Memory** under **Resources** > **Advanced**.

On Windows with the Windows Subsystem for Linux (WSL 2) backend, set the limit in the [`.wslconfig` file](https://learn.microsoft.com/en-us/windows/wsl/wsl-config) instead.

![Docker Desktop Preferences showing the Memory slider under Resources > Advanced](../assets/tutorials/launch/DockerConfigMac.png)

Docker Engine on Linux uses the host's memory directly, so it has no separate limit.

## Monitor Docker container memory

This section applies to the [pre-built Docker image](../getting-started/docker-install.md).

To see your [Docker](https://www.docker.com/) configuration, run in a terminal:

```bash
docker info | grep Memory
```

To see the memory that running containers use, run the [Docker stats](https://docs.docker.com/engine/reference/commandline/stats/) built-in command:

```bash
docker stats
```

This shows the live memory usage of all active containers, as well as each container's memory limit. A container with no limit set shows the memory available to Docker.

The `MEM USAGE / LIMIT` column shows values such as `20.00MiB / 5.804GiB`. In that value, the first number is the RAM the container uses, and the second is its limit. For example, a container that uses `20.00MiB` of RAM out of its `5.804GiB` limit has room to grow. While your session runs, watch this column to see which containers might need more memory.

## Related documentation

- [Install guide for Docker](../getting-started/docker-install.md)
- [Build and launch Deephaven from source code](../getting-started/launch-build.md)
- [Install Deephaven with pip](../getting-started/pip-install.md)
- [Configure the Docker application](./configuration/docker-application.md)
- [Configure the production application](./configuration/configure-production-application.md)
- [Configuration properties](./configuration/configuration-properties.md)
