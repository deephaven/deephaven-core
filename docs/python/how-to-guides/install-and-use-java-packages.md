---
title: Install and use Java packages
---

You can add JARs to the classpath of the Deephaven Community server process (the worker that runs your queries) to make Java packages available to your queries. A JAR is a Java archive file that packages compiled classes. The classpath is the list of locations that the Java Virtual Machine (JVM) loads classes from. This guide covers four deployment options: Docker, pip-installed Deephaven, the production application, and building from source. After you install the JARs with any of them, see [Use the package](#use-the-package) and [Use Java packages in query strings](#use-java-packages-in-query-strings).

Once installed, you can access a package's classes from Python with [jpy](./use-jpy.md), the Python-to-Java bridge. You can also use the classes in [query strings](./query-string-overview.md), the string formulas you pass to [table operations](../conceptual/table-operations-overview.md).

> [!CAUTION]
> Be careful when adding JARs to Deephaven workers. Java packages may be missing dependencies or have dependencies that conflict with Deephaven's.

The examples in this guide add the [Plexus Common Utilities](https://codehaus-plexus.github.io/plexus-utils/) library to a Deephaven Community instance.

## Docker

If you [run Deephaven with Docker](../getting-started/docker-install.md), you can either build a custom Docker image or mount a volume containing the JAR into the container.

### Build a custom Docker image

To build a custom Docker image, create a Dockerfile that downloads the JAR and adds it to the image. The following `Dockerfile` starts from the `server` image, which runs the Python console, and adds `plexus-utils-4.0.2.jar` to `/apps/libs` in the image. The Deephaven Docker images include `/apps/libs/*` in the JVM classpath at startup:

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
```

### Mount the JAR into the container manually

Adding JARs to a Docker container does not require building a custom image. Instead, you can mount a folder into the container that contains whatever JARs you wish to use. Mount your local JAR directory to `/apps/libs`, which the images already include on the classpath, so the JARs are available when the container starts.

The following command assumes you've placed the JAR file into a folder called `/home/user/java/libs`:

```bash
docker run --rm -v /home/user/java/libs:/apps/libs -p 10000:10000 ghcr.io/deephaven/server:latest
```

Next, see [Use the package](#use-the-package).

## pip-installed Deephaven

Unlike Docker, [pip-installed Deephaven](../getting-started/pip-install.md) runs as a process on the host, so `extra_classpath` can point to any JAR that the user running Python can read. Pass the JARs you want to use to the `extra_classpath` argument of `Server`. This argument takes a list of paths or glob patterns, and Deephaven expands each glob to the matching files. The following Python code assumes you've placed the JARs in `/home/user/java/libs/`.

The `-DAuthHandlers` argument in `jvm_args` enables anonymous access, so you can connect without a pre-shared key. Use anonymous access for local testing only:

<!-- This test is skipped because we don't support testing pip-installed Deephaven -->

```python skip-test
from deephaven_server import Server

s = Server(
    port=10000,
    jvm_args=[
        "-Xmx4g",
        "-DAuthHandlers=io.deephaven.auth.AnonymousAuthenticationHandler",
    ],
    extra_classpath=["/home/user/java/libs/*.jar"],
)
s.start()
```

To list each JAR instead of using a glob pattern, pass the paths in order, such as `extra_classpath=["/home/user/java/libs/plexus-utils-4.0.2.jar", ...]`.

Next, see [Use the package](#use-the-package).

## Production application

The [Production application](../getting-started/production-application.md) uses the environment variable `EXTRA_CLASSPATH` to include additional JARs in the classpath. Set it in the environment where the production application runs, for example `EXTRA_CLASSPATH="/path/to/libs/*"`. See [Configure the production application](./configuration/configure-production-application.md#environment-variables) for more details, including other configuration options.

Next, see [Use the package](#use-the-package).

## Build from source

As with the [production application](#production-application), you add JARs to [Deephaven built from source code](../getting-started/launch-build.md) by setting the `EXTRA_CLASSPATH` environment variable. The difference is how you launch the server.

You still follow all the steps up to and including [Build and install the wheel](../getting-started/launch-build.md#build-and-install-the-wheel). Then, instead of running `./gradlew server-jetty-app:run`, run this:

```bash
./gradlew server-jetty-app:installDist
```

Running `installDist` creates the directory `./server/jetty-app/build/install/server-jetty/bin`, which contains a `start` script.

The Python server runs Python from the virtual environment that you created for the wheel. Before you run the `start` script, open a new shell and run `source ~/my-dh-venv/bin/activate`.

Set the `EXTRA_CLASSPATH` environment variable when you run the `start` script.

> [!CAUTION]
> Use quotes around the classpath value to prevent shell expansion of asterisks. The JVM needs to receive the literal `*` character to include all JARs in the directory.

You can pass it directly to the command:

```bash
EXTRA_CLASSPATH="/path/to/libs/*" ./server/jetty-app/build/install/server-jetty/bin/start
```

Or you can export the environment variable before running the script:

```bash
export EXTRA_CLASSPATH="/path/to/libs/*"
./server/jetty-app/build/install/server-jetty/bin/start
```

Next, see [Use the package](#use-the-package).

## Use the package

Once a JAR is on the classpath, get the Java class with [`jpy.get_type`](./use-jpy.md) and call its methods from Python. [`jpy.get_type`](./use-jpy.md) takes the fully qualified class name. The following code gets the [`org.codehaus.plexus.util.StringUtils`](https://codehaus-plexus.github.io/plexus-utils/apidocs/org/codehaus/plexus/util/StringUtils.html) class and calls its static `abbreviate` method:

<!-- This test is skipped because it requires installing the Plexus Common Utilities JAR. -->

```python skip-test
import jpy

StringUtils = jpy.get_type("org.codehaus.plexus.util.StringUtils")
print(StringUtils.abbreviate("Hello world", 9))  # Prints: Hello ...
```

## Use Java packages in query strings

You can also call the package's classes in [query strings](./query-string-overview.md), the string formulas you pass to [table operations](../conceptual/table-operations-overview.md) such as [`update`](../reference/table-operations/select/update.md).

Query strings need the full class name, such as `org.codehaus.plexus.util.StringUtils`, unless you import the class into the query library first. To import a class, use [`query_library.import_class`](./java-classes.md). For example, `query_library.import_class("org.codehaus.plexus.util.StringUtils")` makes the short name `StringUtils` available to query strings.

The following code calls [`org.codehaus.plexus.util.StringUtils.abbreviate`](https://codehaus-plexus.github.io/plexus-utils/apidocs/org/codehaus/plexus/util/StringUtils.html#abbreviate(java.lang.String,int)) from the [Plexus Common Utilities](https://codehaus-plexus.github.io/plexus-utils/) library to abbreviate a string. The example creates a one-row table with [`empty_table`](../reference/table-operations/create/emptyTable.md) and adds a column that calls the method.

<!-- This test is skipped because it requires installing the Plexus Common Utilities JAR. -->

```python skip-test
from deephaven import empty_table

source = empty_table(1).update(
    [
        "StringColumn = `Hello world`",
        "Abbreviated = org.codehaus.plexus.util.StringUtils.abbreviate(StringColumn, 9)",
    ]
)  # Abbreviated is "Hello ..."
```

## Related documentation

- [Query strings](./query-string-overview.md)
- [How to install packages](./install-packages.md)
- [How to configure the Deephaven Docker application](./configuration/docker-application.md)
- [Launch Deephaven from pre-built images](../getting-started/docker-install.md)
- [Install and run the Deephaven production application](../getting-started/production-application.md)
- [Configure the Deephaven production application](./configuration/configure-production-application.md)
- [Build and launch Deephaven from source code](../getting-started/launch-build.md)
- [How to use jpy](./use-jpy.md)
- [How to use Java classes](./java-classes.md)
- [Create an empty table](./new-and-empty-table.md#empty_table)
- [`empty_table`](../reference/table-operations/create/emptyTable.md)
- [`update`](../reference/table-operations/select/update.md)
- [Pydoc](/core/pydoc/code/deephaven.table_factory.html#deephaven.table_factory.empty_table)
- [Access your file system with Docker bind mounts](../conceptual/docker-data-volumes.md)
