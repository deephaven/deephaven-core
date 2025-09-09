---
title: Install and use Java packages
---

You can add JARs to the classpath in Deephaven Community workers to make Java packages available to your queries. All of Deephaven's deployment options support doing this; they are all covered in this guide.

Once a package is installed, it can be imported and used like any other Java package.

> [!CAUTION]
> Care should be taken when adding JARs to Deephaven workers. Java packages may be missing dependencies, or have dependencies that conflict with Deephaven's.

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

Adding JARs to a Docker container does not require you to build a custom image. Instead, you can simply mount a folder into the container that contains whatever JARS you wish to use.

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

The [Production application](../tutorials/production-application.md) uses the environment variable `EXTRA_CLASSPATH` to include additional JARs in the classpath. See [Configure the production application](./configuration/configure-production-application.md#environment-variables) for more details including other configuration options.

## Build from source

<!-- TODO: The instructions in the existing guide don't work, and EXTRA_CLASSPATH doesn't work either. Figure out how to do this. -->

## Use Java packages in query strings

Not only can you import and use the extra Java packages in normal Groovy code, but you can also call them in query strings. You must provide the full package name unless you construct an instance of the class beforehand. The following code calls [`org.codehaus.plexus.util.StringUtils.abbreviate`](<https://codehaus-plexus.github.io/plexus-utils/apidocs/org/codehaus/plexus/util/StringUtils.html#abbreviate(java.lang.String,int)>) from the [Plexus Common Utilities](https://codehaus-plexus.github.io/plexus-utils/) library to abbreviate a string.

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
