---
title: Configure the Deephaven Docker application
---

This guide shows how to run the Deephaven Docker image with custom JVM options, configuration, and ports. It also covers the following tasks:

- Setting JVM options.
- Using a configuration file.
- Changing the port.
- Building a custom image.
- Importing custom JARs.
- Running the image with Docker Compose.
- Debugging a running container.

The sections on image contents, versioning, and available images come after the task sections.

Deephaven can be run from [Docker](https://www.docker.com/) alone or with [Docker Compose](https://docs.docker.com/compose/). Deephaven recommends Docker Compose for customized applications, because it provides a simpler way to orchestrate complex applications with multiple services.

This guide assumes some knowledge of [Docker](https://docs.docker.com/). For lower-level details, such as the `start` script, JVM options, and the directories the images use, see [Configure the production application](./configure-production-application.md).

The image build logic is in the [deephaven-server-docker](https://github.com/deephaven/deephaven-server-docker) repository.

## Configuration mechanisms

The image accepts configuration through these main mechanisms:

| Mechanism                                | Where you set it                                                                    | Use it for                                                                       |
| ---------------------------------------- | ----------------------------------------------------------------------------------- | -------------------------------------------------------------------------------- |
| `START_OPTS`                             | Environment variable                                                                | Adding JVM arguments and system properties to the defaults.                      |
| `JAVA_OPTS`                              | Environment variable                                                                | Replacing the default JVM options file, such as to change the garbage collector. |
| [Configuration file](./config-file.md)   | `/opt/deephaven/config/deephaven.prop`, mounted with `-v` or copied in a Dockerfile | Setting many Deephaven properties.                                               |
| Docker Compose `environment` and `ports` | Compose file or shell                                                               | Setting values for one service in a Compose application.                         |

## Run with Docker

The quickest way to get up and running with the Deephaven Docker application is to run it with the default configuration:

```bash
docker run --rm -p 10000:10000 ghcr.io/deephaven/server:latest
```

## Set JVM options

To [add additional JVM arguments](#configuration-mechanisms) to the default configuration, set `START_OPTS`. The following `START_OPTS` tells the JVM to start with 4GB of heap memory, but to be able to use up to 6GB if necessary:

```bash
docker run --rm -p 10000:10000 --env START_OPTS="-Xms4g -Xmx6g" ghcr.io/deephaven/server:latest
```

The defaults already select the G1 garbage collector. The following `START_OPTS` tries to select the [Z Garbage Collector](https://docs.oracle.com/en/java/javase/21/gctuning/z-garbage-collector.html) as well, so the JVM fails to start:

```bash
$ docker run --rm --env "START_OPTS=-XX:+UseZGC" ghcr.io/deephaven/server:latest
Error occurred during initialization of VM
Multiple garbage collectors selected
```

To use the Z Garbage Collector, set `JAVA_OPTS` instead. Setting `JAVA_OPTS` replaces the image's VM options file, `/opt/deephaven/server-jetty/etc/dh-default.vmoptions`. That file sets these defaults:

- The G1 garbage collector
- The pause-time target
- String deduplication
- `-XX:GCLockerRetryAllocationCount=128`

Include any of those settings you still want:

```bash
docker run --rm -p 10000:10000 --env "JAVA_OPTS=-XX:+UseZGC" ghcr.io/deephaven/server:latest
```

> [!CAUTION]
> `JAVA_OPTS` replaces the image's VM options file, so any default option you leave out is lost. `START_OPTS` keeps the defaults, so use it unless you need to change them. Take care when overriding default VM options.

`START_OPTS` takes JVM arguments, including system properties, which are JVM `-D` settings. If you set many Deephaven configuration values this way, use a [configuration file](./config-file.md) instead.

## Use a configuration file

Mount your file over the image's copy with `-v`. This bind mount maps a host file or folder into the container. Start the file with `includefiles=dh-defaults.prop` so it keeps the default properties, and then add your own properties:

```bash
docker run --rm -p 10000:10000 -v /path/to/deephaven.prop:/opt/deephaven/config/deephaven.prop ghcr.io/deephaven/server:latest
```

## Change the port

The images expose port `10000` by default. To change the host port, use a port mapping such as `-p 10001:10000`.

To serve on a different port inside the container, set the `http.port` property in a [configuration file](./config-file.md). The image's health check probes port `10000`, so the container reports as unhealthy after you change `http.port`.

With [Docker Compose](https://docs.docker.com/compose/), set the `DEEPHAVEN_PORT` shell variable to change the host port. Compose reads it when it substitutes values into the Compose file:

```bash
export DEEPHAVEN_PORT=10001
```

To change the default port in the Compose file instead, change the fallback value in the `ports` entry:

```yaml
ports:
  - "${DEEPHAVEN_PORT:-10001}:10000"
```

## Customize the image

You can build your own image that includes your configuration:

```docker title="Dockerfile"
FROM ghcr.io/deephaven/server:latest
COPY deephaven.prop /opt/deephaven/config/deephaven.prop
ENV START_OPTS="-Xmx2g"
ENV EXTRA_CLASSPATH="/apps/libs/*:/opt/more_java_libs/*"
```

The `COPY` instruction replaces the image's configuration file. Start your `deephaven.prop` with `includefiles=dh-defaults.prop`, as described in [Use a configuration file](#use-a-configuration-file), and then add your own properties.

The second classpath entry, `/opt/more_java_libs`, is an example. Replace it with the directory where your Dockerfile places your JARs.

```bash
# Build the image from the Dockerfile
docker build -t my-deephaven-image .
# Run the image
docker run --rm -p 10000:10000 my-deephaven-image
```

### Add Python packages

Deephaven's Python Docker images include a [virtual environment](https://docs.python.org/3/library/venv.html) in `/opt/deephaven/venv` and set `VIRTUAL_ENV=/opt/deephaven/venv`. Install additional Python packages in your own image:

```docker title="Dockerfile"
FROM ghcr.io/deephaven/server:latest
RUN pip install some-awesome-package
```

## Import custom JARs

You can make your own Java classes (and third-party libraries) available to queries by placing JARs under `/apps/libs`. The images add that directory to the classpath (see [Image contents and startup settings](#image-contents-and-startup-settings)).

<details>
<summary>Option A: bind mount</summary>

```bash
mkdir -p jars
cp /path/to/<custom>.jar jars/
docker run --rm -p 10000:10000 -v "$(pwd)/jars:/apps/libs" ghcr.io/deephaven/server:latest
```

</details>

<details>
<summary>Option B: Docker Compose</summary>

If you run with [Docker Compose](#run-with-docker-compose), mount the folder in the service definition:

```yaml title="docker-compose.yml"
services:
  deephaven:
    image: ghcr.io/deephaven/server:latest
    ports:
      - "10000:10000"
    volumes:
      - ./jars:/apps/libs
```

</details>

You can run queries that use your custom JARs. For example, if you have a class `org.example.MathFns` with a static method `square(long x)`, you can run the following query:

```python skip-test
# Example usage (requires custom JAR with org.example.MathFns class)
from deephaven import empty_table

table = empty_table(5).update(["Squares = org.example.MathFns.square(i)"])
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

`VERSION` sets the image tag. It defaults to `latest`. Set it in the shell, for example with `export VERSION=42.7`, to pin a version. See [Versioning](#versioning).

To change the port, see [Change the port](#change-the-port).

To build the image from a Dockerfile in the same directory, replace the `image` line with `build: .` in the example above.

To run a Docker Compose application, use:

```bash
docker compose up
```

Deephaven offers a variety of pre-built [Docker Compose files](https://github.com/deephaven/deephaven-core/tree/main/containers) that can be used to quickly get up and running with Deephaven. They cover both the Python and Groovy APIs. They also add services, such as example data and [Redpanda](https://redpanda.com/), and extended Python images.

## Debug a running container

To debug a container started with `docker run`, use the `docker exec` command. The following command creates an executable bash shell in a running Docker container called `deephaven-application`. Start the container with `--name deephaven-application` to use this name, or replace the name with the container name or ID that `docker ps` shows:

```bash
docker exec -it deephaven-application /bin/bash
```

With [Docker Compose](https://docs.docker.com/compose/), run `docker compose exec deephaven /bin/bash`.

## Image contents and startup settings

Deephaven's `server` Docker image is based on the [production application](../../getting-started/production-application.md), which is unpacked into the `/opt/deephaven` directory. The images have two [volumes](https://docs.docker.com/storage/volumes/):

- `/data`
- `/cache`

The images expose port `10000` by default. For changing the port, see [Change the port](#change-the-port).

A configuration file exists at `/opt/deephaven/config/deephaven.prop`. It includes the default configuration file `dh-defaults.prop` and sets `deephaven.console.type` to `python`.

Add additional Java JARs to `/apps/libs`, as described in [Import custom JARs](#import-custom-jars).

By default, Deephaven's Docker images set the following environment variables:

- `DEEPHAVEN_DATA_DIR=/data`
- `DEEPHAVEN_CACHE_DIR=/cache`
- `DEEPHAVEN_CONFIG_DIR=/opt/deephaven/config`
- `EXTRA_CLASSPATH=/apps/libs/*`

The first three are [bootstrap configuration](./configure-production-application.md#deephaven-server-bootstrap-configuration) parameters, which the server reads at startup before it reads any configuration file. The `start` script reads `EXTRA_CLASSPATH`.

The JVM arguments and how to change them are described in [Set JVM options](#set-jvm-options).

## Versioning

The examples on this page use the `latest` version for Docker images. Deephaven recommends staying on the `latest` version to get the latest features, bug fixes, and patches. Here is a list of available versions:

- [`latest`](https://github.com/deephaven/deephaven-core/releases/latest): The latest release of Deephaven.
- [`edge`](https://github.com/deephaven/deephaven-core/pkgs/container/server): Server code merged to the `main` branch of deephaven-core at the time the image was built, which may include breaking changes.
- [`42.7`](https://github.com/deephaven/deephaven-core/releases/tag/v42.7) or [`41.11`](https://github.com/deephaven/deephaven-core/releases/tag/v41.11): A specific version number.

Older versions of Deephaven are likely to be incompatible with code found in documentation.

The `edge` image is rebuilt nightly, and whenever the `main` branch of the [deephaven-server-docker](https://github.com/deephaven/deephaven-server-docker) repository changes. It is not rebuilt on every push to deephaven-core `main`. Use it only to try an unreleased feature or develop against the latest code.

## Available images

### Standard images

- `ghcr.io/deephaven/server`: Deephaven's server-side Python API. Includes jpy, `deephaven-plugin`, numpy, pandas, pyarrow, and the `deephaven.ui`, Deephaven Express (`deephaven-plugin-plotly-express`), and Matplotlib plugins. Numba is bundled in `edge`; release `42.7` requires a separate installation.
- `ghcr.io/deephaven/server-ui`: The same image as `ghcr.io/deephaven/server`, under a separate name.

For the Groovy image, `ghcr.io/deephaven/server-slim`, see the Groovy version of this guide.

### Extended Python images

These images include the core Python API (`deephaven-core`) plus the listed libraries. They do not include the plugins bundled in `ghcr.io/deephaven/server`, such as `deephaven.ui`, Deephaven Express (`deephaven-plugin-plotly-express`), and Matplotlib.

- `ghcr.io/deephaven/server-all-ai`: Deephaven's server-side API with NLTK, TensorFlow, PyTorch, and scikit-learn.
- `ghcr.io/deephaven/server-nltk`: Deephaven's server-side API with NLTK.
- `ghcr.io/deephaven/server-pytorch`: Deephaven's server-side API with PyTorch and TensorBoard.
- `ghcr.io/deephaven/server-sklearn`: Deephaven's server-side API with scikit-learn.
- `ghcr.io/deephaven/server-tensorflow`: Deephaven's server-side API with TensorFlow and TensorBoard.

## Related documentation

- [Production application](../../getting-started/production-application.md)
- [Configure the production application](./configure-production-application.md)
- [Deephaven configuration files](./config-file.md)
- [Build from source](../../getting-started/launch-build.md)
