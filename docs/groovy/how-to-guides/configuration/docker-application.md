---
title: Configure the Deephaven Docker application
sidebar_label: Deephaven Docker application
---

This guide covers best practices for configuring the Deephaven Docker application. Deephaven can be run from [Docker](https://www.docker.com/) alone or with [Docker Compose](https://docs.docker.com/compose/). The latter is recommended for customized applications, as it provides a simpler mechanism for orchestrating complex applications with multiple services.

If you are interested in lower-level details, which may be necessary if you are interested in building your own Docker images from scratch, see the [production application guide](./configure-production-application.md). Some knowledge about [Docker](https://docs.docker.com/) is assumed.

## Construction

Deephaven's `server-slim` Docker image is based on the [production application](../../getting-started/production-application.md), which is unpacked into the `/opt/deephaven` directory. The images have two [volumes](https://docs.docker.com/storage/volumes/):

- `/data`
- `/cache`

The images expose port `10000` to users by default. This value can be changed. Additionally, a configuration file exists at `/opt/deephaven/config/deephaven.prop`, which contains the default configuration values for the Deephaven server. Additional Java JARs can be included in `/apps/libs`.

Deephaven's Docker images, by default, set the following bootstrap environment configuration values:

- `DEEPHAVEN_DATA_DIR=/data`
- `DEEPHAVEN_CACHE_DIR=/cache`
- `DEEPHAVEN_CONFIG_DIR=/opt/deephaven/config`
- `EXTRA_CLASSPATH=/apps/libs/*`

Deephaven Docker images can set two environment variables to override or add additional JVM parameters. The semantics for these options remain the same as for the production application:

- `JAVA_OPTS`: Custom JVM properties that override default values.
- `START_OPTS`: Additional JVM properties that are appended to the default values.

> [!CAUTION]
> `START_OPTS` is recommended over `JAVA_OPTS` because it is safer. Take care when overriding default properties.

For more information on the exact construction of the images, the image building logic is located in the [deephaven-server-docker](https://github.com/deephaven/deephaven-server-docker) GitHub repository.

## Versioning

Everything below will use the `latest` version for Docker images. Deephaven recommends staying on the `latest` version to get the latest features, bug fixes, and patches. Here is a list of available versions:

- [`latest`](https://github.com/deephaven/deephaven-core/releases/latest): The latest release of Deephaven.
- [`edge`](https://github.com/deephaven/deephaven-core/pkgs/container/server/240348207?tag=edge): Everything new in the server up until midnight of the previous day. This version may contain features not yet released in `latest` that include breaking changes. This version is only recommended if you wish to try an unreleased feature or develop against the latest code.
- Specific version number: You can also uses a specific version, such as [`0.35.0`](https://github.com/deephaven/deephaven-core/releases/tag/v0.35.0), [`0.34.2`](https://github.com/deephaven/deephaven-core/releases/tag/v0.34.2), and more. Older versions of Deephaven are likely to be incompatible with code found in documentation.

## Run with Docker

> [!NOTE]
> Deephaven recommends running with the `latest` image to stay up-to-date with the latest features, bug fixes, and patches.

The quickest way to get up and running with the Deephaven Docker application is to run it with the default configuration:

```bash
docker run --rm -p 10000:10000 ghcr.io/deephaven/server-slim:latest
```

To [add additional JVM arguments](#construction) to the default configuration, set `START_OPTS`. The following `START_OPTS` tells the JVM to start with 4GB of heap memory, but to be able to use up to 6GB if necessary:

```bash
docker run --rm --env START_OPTS="-Xms4g -Xmx6g" ghcr.io/deephaven/server-slim:latest
```

It's possible that some options conflict with the Deephaven recommendations. The following `START_OPTS` tells the JVM to use the [Z Garbage Collector](https://docs.oracle.com/en/java/javase/21/gctuning/z-garbage-collector.html), but this conflicts with Deephaven's recommendations:

```bash
$ docker run --rm --env "START_OPTS=-XX:+UseZGC" ghcr.io/deephaven/server-slim:latest
Error occurred during initialization of VM
Multiple garbage collectors selected
```

In this case, use `JAVA_OPTS` to override the default value:

```bash
docker run --rm --env "JAVA_OPTS=-XX:+UseZGC" ghcr.io/deephaven/server-slim:latest
```

`START_OPTS` is a convenient way to set system properties. If you find yourself (ab)using system properties to set a lot of Deephaven configuration file values, consider instead overriding the default properties with a custom [configuration file](./config-file.md):

```bash
docker run --rm -v /path/to/deephaven.prop:/opt/deephaven/config/deephaven.prop ghcr.io/deephaven/server-slim:latest
```

You can also build your own image with all of the necessary configuration options baked in:

```docker title="Dockerfile"
FROM ghcr.io/deephaven/server-slim:latest
COPY deephaven.prop /opt/deephaven/config/deephaven.prop
ENV START_OPTS="-Xmx2g"
ENV EXTRA_CLASSPATH="/apps/libs/*:/opt/more_java_libs/*"
```

<></>

```bash
docker build -t my-deephaven-image .
docker run --rm -p 10000:10000 my-deephaven-image
```

### Standard images

- `ghcr.io/deephaven/server`: Deephaven's server-side Python API. Includes jpy, deephaven-plugin, numpy, pandas, numba, and deephaven-ui.
- `ghcr.io/deephaven/server-slim`: Deephaven's server-side Groovy API.

### Extended Python images

- `ghcr.io/deephaven/server-all-ai`: Deephaven's server-side API with NLTK, Tensorflow, PyTorch, and SciKit-Learn.
- `ghcr.io/deephaven/server-nltk`: Deephaven's server-side API with NLTK.
- `ghcr.io/deephaven/server-pytorch`: Deephaven's server-side API with PyTorch.
- `ghcr.io/deephaven/server-sklearn`: Deephaven's server-side API with SciKit-Learn.
- `ghcr.io/deephaven/server-tensorflow`: Deephaven's server-side API with Tensorflow.

### Debugging

To debug a running Docker container, use the `docker exec` command. The following command creates an executable bash shell in a running Docker container called `deephaven-application`:

```bash
docker exec -it deephaven-application /bin/bash
```

## Import custom JARs

You can make your own Java classes (and third-party libraries) available to queries by placing JARs under `/apps/libs`. The images add `/apps/libs/*` to the JVM classpath at startup.

<details>
<summary>Option A - Bind mount</summary>

```bash
$ mkdir -p jars
$ cp /path/to/<custom>.jar jars/

# Will use python console as default
$ docker run --rm -p 10000:10000 -v "$(pwd)/jars:/apps/libs" ghcr.io/deephaven/server:latest
```

</details>

<details>
<summary>Option B - Docker Compose</summary>

```yaml title="docker-compose.yml"
services:
  deephaven:
    image: ghcr.io/deephaven/server:latest
    ports:
      - "10000:10000"
    volumes:
      - ./jars:/apps/libs
    # Optional: choose Groovy console by default
    environment:
      - START_OPTS=-Ddeephaven.console.type=groovy
```

</details>

Now you can run queries that use your custom JARs. For example, if you have a class `org.example.MathFns` with a static method `square(long x)`, you can run the following query:

```groovy skip-test
// Example usage (requires custom JAR with org.example.MathFns class)
table = emptyTable(5).update(
    "Squares = org.example.MathFns.square(i)"
)
```

## Run with Docker Compose

Deephaven recommends running with [Docker Compose](https://docs.docker.com/compose/), as it makes it easier to run Deephaven with additional services, such as:

- Kafka brokers.
- Backend databases.
- Frontend applications.

Docker Compose files are written in YAML. The most basic Docker Compose file for Deephaven with Python is:

```yaml
services:
  deephaven:
    image: ghcr.io/deephaven/server:${VERSION:-latest}
    ports:
      - "${DEEPHAVEN_PORT:-10000}:10000"
    volumes:
      - ./data:/data
    environment:
      - START_OPTS=-Xmx4g
```

To make Deephaven accessible on a different port, set the `DEEPHAVEN_PORT` environment variable:

```bash
export DEEPHAVEN_PORT=10001
```

Or set it in the Docker Compose file:

```yaml
services:
  deephaven:
    image: ghcr.io/deephaven/server:${VERSION:-latest}
    ports:
      - "${DEEPHAVEN_PORT:-10001}:10000"
    volumes:
      - ./data:/data
    environment:
      - START_OPTS=-Xmx4g
```

To make a Docker Compose application depend on an image built by a Dockerfile in the same directory, use the `build` key:

```yaml
services:
  deephaven:
    build: .
    ports:
      - "${DEEPHAVEN_PORT:-10000}:10000"
    volumes:
      - ./data:/data
    environment:
      - START_OPTS=-Xmx4g
```

Running any Docker compose application is as simple as:

```bash
docker compose up
```

Deephaven offers a variety of pre-built [Docker Compose files](https://github.com/deephaven/deephaven-core/tree/main/containers) that can be used to quickly get up and running with Deephaven. They cover both the Python and Groovy API and add more services including example data, extended Python images, and [Redpanda](https://redpanda.com/).

## Related documentation

- [Configure the production application](./configure-production-application.md)
- [Deephaven configuration files](./config-file.md)
- [Build from source](../../getting-started/launch-build.md)
