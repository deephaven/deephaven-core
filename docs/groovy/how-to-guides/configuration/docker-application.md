---
title: Configure the Deephaven Docker application
sidebar_label: Deephaven Docker application
---

This documentation provides user-level details about the Deephaven Docker application construction and configuration. If you are interested in the lower-level details (which may be necessary if you are interested in building your own Docker images from scratch), please see the [production application documentation](./configure-production-application.md). Some knowledge about [Docker](https://docs.docker.com/) is assumed.

## Construction

The Deephaven Docker application images are based on the [production application](../../tutorials/production-application.md), which is unpacked into the `/opt/deephaven` directory. The images have volumes `/data` and `/cache`. The images expose port 10000. A configuration file exists at `/opt/deephaven/config/deephaven.prop`. Additional Java JARs can be included in `/apps/libs`.

As such, the Deephaven Docker application images set the following bootstrap environment configuration values:

- `DEEPHAVEN_DATA_DIR=/data`
- `DEEPHAVEN_CACHE_DIR=/cache`
- `DEEPHAVEN_CONFIG_DIR=/opt/deephaven/config`
- `EXTRA_CLASSPATH=/apps/libs/*`

The Deephaven Docker application images may set the environment variable `JAVA_OPTS`. The semantics for these options remain the same as it does for the production application — if defined, they are Deephaven recommended arguments that the user can override if necessary. The Deephaven Docker application images will not set the environment variable `START_OPTS`.

The Python images further have a virtual environment in `/opt/deephaven/venv` and set the environment value `VIRTUAL_ENV=/opt/deephaven/venv`.

For more information on the exact construction of the images, the image building logic is located in the repository [deephaven-server-docker](https://github.com/deephaven/deephaven-server-docker).

## Configuration

The quickest way to get up and running with the Deephaven Docker application is to run it with the default configuration:

```bash
docker run --rm -p 10000:10000 ghcr.io/deephaven/server:latest
```

In general, to add additional JVM arguments you'll use `START_OPTS`.

```bash
docker run --rm --env START_OPTS="-Xms4g -Xmx4g" ghcr.io/deephaven/server:latest
```

It's possible that some options you might want to set conflict with the Deephaven recommendations:

```bash
$ docker run --rm --env "START_OPTS=-XX:+UseZGC" ghcr.io/deephaven/server:latest
Error occurred during initialization of VM
Multiple garbage collectors selected
```

In this case, you'd need to override `JAVA_OPTS`:

```bash
docker run --rm --env "JAVA_OPTS=-XX:+UseZGC" ghcr.io/deephaven/server:latest
```

While you can use `START_OPTS` to set system properties, if you find yourself (ab)using system properties to set a lot of Deephaven configuration file values, it may be best to extend the Deephaven image with your own values:

```bash
docker run --rm -v /path/to/deephaven.prop:/opt/deephaven/config/deephaven.prop ghcr.io/deephaven/server:latest
```

You can also build your own image with all of the necessary configuration options baked in:

```docker title="Dockerfile"
FROM ghcr.io/deephaven/server:latest
COPY deephaven.prop /opt/deephaven/config/deephaven.prop
ENV START_OPTS="-Xmx2g"
ENV EXTRA_CLASSPATH="/apps/libs/*:/opt/more_java_libs/*"
```

<></>

```bash
docker build -t my-deephaven-image .
docker run --rm -p 10000:10000 my-deephaven-image
```

You can also install additional Python dependencies into your own image if desired:

```docker title="Dockerfile"
FROM ghcr.io/deephaven/server:latest
RUN pip install some-awesome-package
```

Of course, all of these various options can be orchestrated via [docker compose](https://docs.docker.com/compose/) as well:

```yaml title="docker-compose.yml"
version: "3"

services:
  deephaven:
    image: ghcr.io/deephaven/server:latest
    ports:
      - "10000:10000"
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

## Standard images

- `ghcr.io/deephaven/server:latest`: Python session based, includes jpy, deephaven-plugin, numpy, pandas, and numba.
- `ghcr.io/deephaven/server-slim:latest`: Groovy session based.

## Extended python images:

- `ghcr.io/deephaven/server-all-ai:latest`: contains the standard packages as well as nltk, tensorflow, torch, and scikit-learn.
- `ghcr.io/deephaven/server-nltk:latest`: contains the standard packages as well as nltk.
- `ghcr.io/deephaven/server-pytorch:latest`: contains the standard packages as well as torch.
- `ghcr.io/deephaven/server-sklearn:latest`: contains the standard packages as well as scikit-learn.
- `ghcr.io/deephaven/server-tensorflow:latest`: contains the standard packages as well as tensorflow.

## Related documentation

- [Configure the production application](./configure-production-application.md)
- [Build from source](../launch-build.md)
