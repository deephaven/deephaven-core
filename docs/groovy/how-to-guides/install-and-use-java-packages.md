---
title: Install and use Java packages
---

You can add JARs to the classpath in Deephaven Community workers to make Java packages available to your queries. All of Deephaven's deployment options support this, and this guide covers them all.

Once installed, a package can be imported and used like any other Java package.

> [!CAUTION]
> Care should be taken when adding JARs to Deephaven workers. Java packages may be missing dependencies or have dependencies that conflict with Deephaven's.

The examples in this guide add the [Plexus Common Utilities](https://codehaus-plexus.github.io/plexus-utils/) library to a Deephaven Community instance.

## Docker

If you [Run Deephaven with Docker](../tutorials/docker-install.md), you can either build a custom Docker image or mount a volume containing the JAR into the container.

### Build a custom Docker image

To build a custom Docker image, create a Dockerfile that downloads the JAR and mounts it into the container. The following `Dockerfile` mounts `plexus-utils-4.0.2.jar` into `/apps/libs` in the container:

```dockerfile
FROM ghcr.io/deephaven/server-slim:latest
RUN curl --output /apps/libs/plexus-utils-4.0.2.jar https://repo1.maven.org/maven2/org/codehaus/plexus/plexus-utils/4.0.2/plexus-utils-4.0.2.jar
```

You then need to build your image. If you use Docker without Compose, run `docker build` and `docker run`:

```bash
docker build --tag deephaven-plexus-utils .
docker run --rm --name deephaven-with-plexus-utils -p 10000:10000 deephaven-plexus-utils
```

Alternatively, you can build your image manually and reference it in a Docker Compose YAML file:

```yaml
services:
  deephaven:
    image: deephaven-plexus-utils
    ports:
      - "${DEEPHAVEN_PORT:-10000}:10000"
    volumes:
      - ./data:/data
    environment:
      - START_OPTS=-Xmx4g
```

Or, if you use Docker Compose, you can have Compose build the image for you. The following YAML assumes that the Dockerfile lives in the same directory:

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

### Mount the JAR into the container manually

Adding JARs to a Docker container does not require building a custom image. Instead, you can simply mount a folder into the container that contains whatever JARs you wish to use.

The following YAML assumes you've placed the `plexus-utils-4.0.2.jar` file into a folder called `./apps/libs`:

```yaml
services:
  deephaven:
    image: ghcr.io/deephaven/server:latest
    ports:
      - "${DEEPHAVEN_PORT:-10000}:10000"
    volumes:
      - ./data:/data
      - ./apps/libs:/apps/libs
    environment:
      - START_OPTS=-Xmx4g
```

## Production application

The [Production application](../tutorials/production-application.md) uses the environment variable `EXTRA_CLASSPATH` to include additional JARs in the classpath. See [Configure the production application](./configuration/configure-production-application.md#environment-variables) for more details, including other configuration options.

## Build from source

Adding Java packages to [Deephaven built from source code](./launch-build.md) is similar to the [production application](#production-application). It does, however, change how you launch Deephaven built from source.

Rather than running [`./gradlew server-jetty-app:run -Pgroovy`](./launch-build.md#build-and-run), run this instead:

```bash
./gradlew server-jetty-app:installDist -Pgroovy
```

This creates the directory `./server/jetty-app/build/install/server-jetty/bin`, which contains a `start` script that you can pass additional parameters to, such as an `EXTRA_CLASSPATH` environment variable. You can pass it directly to the command:

```bash
EXTRA_CLASSPATH=/path/to/libs/*:/apps/libs/* ./server/jetty-app/build/install/server-jetty/bin/start
```

Or you can export the environment variable before running the script:

```bash
export EXTRA_CLASSPATH=/path/to/libs/*:/apps/libs/*
./server/jetty-app/build/install/server-jetty/bin/start
```

> [!CAUTION]
> Some shells expand asterisks. Check the value of your environment variable to ensure it is correct.

## Use Java packages in query strings

Not only can you import and use the extra Java packages in normal Groovy code, but you can also call them in query strings. You'll need to provide the full package name unless you construct an instance of the class beforehand. The following code calls [`org.codehaus.plexus.util.StringUtils.abbreviate`](https://codehaus-plexus.github.io/plexus-utils/apidocs/org/codehaus/plexus/util/StringUtils.html#abbreviate(java.lang.String,int)) from the [Plexus Common Utilities](https://codehaus-plexus.github.io/plexus-utils/) library to abbreviate a string.

<!-- This test is skipped because it requires installing the Plexus Common Utilities JAR. -->

```groovy skip-test
source = emptyTable(1).update(
    "StringColumn = `Hello world`",
    "Abbreviated = org.codehaus.plexus.util.StringUtils.abbreviate(StringColumn, 9)"
)
```

## Related documentation

- [How to install packages](./install-packages.md)
- [How to configure the Deephaven Docker application](./configuration/docker-application.md)
- [Launch Deephaven from pre-built images](../tutorials/docker-install.md)
- [Build and launch Deephaven from source code](./launch-build.md)
- [Access your file system with Docker data volumes](../conceptual/docker-data-volumes.md)
