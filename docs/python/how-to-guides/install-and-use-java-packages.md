---
title: Install and use Java packages
---

You can add JARs to the classpath in Deephaven Community workers to make Java packages available to your queries. All of Deephaven's deployment options support this, and this guide covers them all.

Once installed, a package can be imported and used like any other Java package.

> [!CAUTION]
> Care should be taken when adding JARs to Deephaven workers. Java packages may be missing dependencies or have dependencies that conflict with Deephaven's.

The examples in this guide add the [Plexus Common Utilities](https://codehaus-plexus.github.io/plexus-utils/) library to a Deephaven Community instance.

## Docker

If you [Run Deephaven with Docker](../getting-started/docker-install.md), you can either build a custom Docker image or mount a volume containing the JAR into the container.

### Build a custom Docker image

To build a custom Docker image, create a Dockerfile that downloads the JAR and adds it to the image. The following `Dockerfile` adds `plexus-utils-4.0.2.jar` to `/apps/libs` in the image. The Deephaven Docker images automatically include `/apps/libs/*` in the JVM classpath at startup:

```dockerfile
FROM ghcr.io/deephaven/server:latest
ADD https://repo1.maven.org/maven2/org/codehaus/plexus/plexus-utils/4.0.2/plexus-utils-4.0.2.jar /apps/libs/plexus-utils-4.0.2.jar
```

You then need to build your image. If you use Docker without Compose, run `docker build` and `docker run`:

```bash
docker build --tag deephaven-plexus-utils .
docker run --rm --name deephaven-with-plexus-utils -p 10000:10000 deephaven-plexus-utils
```

Alternatively, if you use Docker Compose, you can have Compose build the image for you. The following YAML assumes that the Dockerfile lives in the same directory:

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

Adding JARs to a Docker container does not require building a custom image. Instead, you can simply mount a folder into the container that contains whatever JARs you wish to use. Since the Deephaven Docker images automatically include `/apps/libs/*` in the JVM classpath, mounting your local JAR directory to `/apps/libs` makes them immediately available.

The following command assumes you've placed the JAR file into a folder called `/home/user/java/libs`:

```bash
docker run --rm -v /home/user/java/libs:/apps/libs -p 10000:10000 ghcr.io/deephaven/server:latest
```

## pip-installed Deephaven (Python)

Unlike Docker, [pip-installed Deephaven](../getting-started/pip-install.md) has visibility to the entire host file system. Thus, you should change the `EXTRA_CLASSPATH` environment variable to point at the directory containing the JARs you wish to use. The following Python code assumes you've placed the JARS in `/home/user/java/libs/`:

<!-- This test is skipped because we don't support testing pip-installed Deephaven -->

```python skip-test
from deephaven_server import Server

s = Server(
    port=10000,
    jvm_args=[
        "-Xmx4g",
        "-DAuthHandlers=io.deephaven.auth.AnonymousAuthenticationHandler",
    ],
    extra_classpath="/home/user/java/libs/",
)
s.start()
```

You can also point `extra_classpath` at one or more specific JARs instead of a folder:

<!-- This test is skipped because we don't support testing pip-installed Deephaven -->

```python skip-test
from deephaven_server import Server

s = Server(
    port=10000,
    jvm_args=[
        "-Xmx4g",
        "-DAuthHandlers=io.deephaven.auth.AnonymousAuthenticationHandler",
    ],
    extra_classpath=[
        "/home/user/java/libs/plexus-utils-4.0.2.jar",
        "/home/user/java/libs/some-other-library-1.0.0.jar",
    ],
)
s.start()
```

## Production application

The [Production application](../getting-started/production-application.md) uses the environment variable `EXTRA_CLASSPATH` to include additional JARs in the classpath. See [Configure the production application](./configuration/configure-production-application.md#environment-variables) for more details, including other configuration options.

## Build from source

Adding Java packages to [Deephaven built from source code](../getting-started/launch-build.md) is similar to the [production application](#production-application). It does, however, change how you launch Deephaven built from source.

You still follow all the steps up to and including [Build and install the wheel](../getting-started/launch-build.md#build-and-install-the-wheel). However, the next step changes. Rather than run `./gradlew server-jetty-app:run`, run this instead:

```bash
./gradlew server-jetty-app:installDist
```

This creates the directory `./server/jetty-app/build/install/server-jetty/bin`, which contains a `start` script that you can pass additional parameters to, such as an `EXTRA_CLASSPATH` environment variable.

> [!CAUTION]
> Use quotes around the classpath value to prevent shell expansion of asterisks. The JVM needs to receive the literal `*` character to include all JARs in the directory.

You can pass it directly to the command:

```bash
EXTRA_CLASSPATH="/path/to/libs/*:/apps/libs/*" ./server/jetty-app/build/install/server-jetty/bin/start
```

Or you can export the environment variable before running the script:

```bash
export EXTRA_CLASSPATH="/path/to/libs/*:/apps/libs/*"
./server/jetty-app/build/install/server-jetty/bin/start
```

## Use Java packages in query strings

Not only can you import and use the extra Java packages in normal Python code, but you can also call them in query strings. You'll need to provide the full package name unless you construct an instance of the class beforehand. The following code calls [`org.codehaus.plexus.util.StringUtils.abbreviate`](https://codehaus-plexus.github.io/plexus-utils/apidocs/org/codehaus/plexus/util/StringUtils.html#abbreviate(java.lang.String,int)) from the [Plexus Common Utilities](https://codehaus-plexus.github.io/plexus-utils/) library to abbreviate a string.

<!-- This test is skipped because it requires installing the Plexus Common Utilities JAR. -->

```python skip-test
from deephaven import empty_table

source = empty_table(1).update(
    [
        "StringColumn = `Hello world`",
        "Abbreviated = org.codehaus.plexus.util.StringUtils.abbreviate(StringColumn, 9)",
    ]
)
```

## Related documentation

- [Create an empty table](./new-and-empty-table.md#empty_table)
- [How to install packages](./install-packages.md)
- [How to configure the Deephaven Docker application](./configuration/docker-application.md)
- [Launch Deephaven from pre-built images](../getting-started/docker-install.md)
- [Build and launch Deephaven from source code](../getting-started/launch-build.md)
- [How to use Java packages in query strings](./install-and-use-java-packages.md#use-java-packages-in-query-strings)
- [How to use jpy](./use-jpy.md)
- [Access your file system with Docker data volumes](../conceptual/docker-data-volumes.md)
