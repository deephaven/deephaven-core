---
title: Attach a JVM debugger
---

While the Deephaven UI offers unrivaled visibility into the state of data-driven applications, some
code problems — particularly those involving custom Java, Groovy, or Python code — are easiest to investigate with
a debugger. This guide will explain how to enable remote debugging of the JVM, including how to expose the remote
debugging port through Docker when necessary.

## Listening for debugger connections

To listen for remote debugger connections, additional options must be added to the JVM start command. When starting Java
manually, add the following the option to the command line:

```shell
-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5005
```

The `-agentlib:jdwp` option enables remote debugging by instructing the JVM to listen for [JDWP](https://docs.oracle.com/en/java/javase/11/docs/specs/jdwp/jdwp-spec.html)
debugger connections. The sub-options `suspend` and `address` help control how the JVM listens for these connections:

- The `suspend=n` sub-option tells the JVM not to wait for a debugger connection before starting. Setting `suspend=y` will
  make the JVM wait until the debugger is connected before initializing the application. Using `suspend=y` is helpful
  when investigating issues that occur during startup, such errors initializing or loading classes.
- The `address=5005` sub-option tells the JVM to listen for debugger connections on port `5005`. (Port `5005` can be
  replaced with any other valid, unused port number greater than `1023`.) By default, the JVM only listens
  on `localhost`. A specific IP address or hostname to listen on can be specified as well
  (e.g., `address=localhost:5005`), or an asterisk (`address=*:5005`) to listen on all network interfaces.

The next section deals with debugging Deephaven within Docker.

## Debugging Deephaven under Docker

When running Deephaven under Docker, the `docker-compose.yml` file must be modified both to include the `-agentlib`
JVM option (which causes the JVM running within Docker to listen for debuggers) and to expose the remote debugging port through
Docker (which allows a debugger running normally on your computer to connect to the JVM running Deephaven inside Docker).

Starting from Deephaven's
standard [docker-compose.yml](https://raw.githubusercontent.com/deephaven/deephaven-core/main/containers/python/base/docker-compose.yml)
file, two changes must be made to enable debugging:

1. Under the `ports` section, add an entry to expose the port on which the JVM will listen for debugger connections. (In
   this guide, port `5005` is used.)
2. Expand the `START_OPTS` environment variable to include the `agentlib` command. For example if the
   `docker-compose.yml` file's `environment` key contains `START_OPTS=-Xmx4g`, it should be expanded to
   `START_OPTS=-Xmx4g -agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=*:5005`. Note that when debugging
   Deephaven running inside a Docker container, the `address` sub-option must include an asterisk, so that the JVM
   listens for debugger connections from outside the Docker container (including from your computer).

> [!NOTE]
> The `START_OPTS` variable should be set only once, but may include multiple JVM options (e.g., both `-Xmx` and
> `-agentlib`). Do not add multiple entries for `START_OPTS` under the `environment` key.

Below is an example of a `docker-compose.yml` file modified with these changes, so that when the container starts, the
JVM running Deephaven will listen for debugger connections on port 5005.

```yaml
services:
  deephaven:
    image: ghcr.io/deephaven/server:latest
    ports:
      - "${DEEPHAVEN_PORT:-10000}:10000"
      - "5005:5005"
    volumes:
      - ./data:/data
    environment:
      - START_OPTS=-Xmx4g -agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=*:5005
```

## Connecting a debugger from IntelliJ IDEA

At Deephaven, we recommend [IntelliJ IDEA](https://www.jetbrains.com/idea/) for Java development. IDEA makes it
easy to [remote debug](https://www.jetbrains.com/help/idea/tutorial-remote-debug.html) a Java process.

1. From the "Run" menu, select "Edit Configurations..."
2. Click the `+` icon to add a new configuration.
3. Select "Remote JVM Debug" as the configuration type.
4. IDEA will create a new configuration entry. Enter a name for it (e.g., "my-debug-configuration") and ensure that the
   same port number matches the one used when starting the JVM. (Notice that under "Command line arguments for remote JVM",
   IDEA automatically generates the appropriate `-agentlib` option to add to the target JVM.)
5. Click OK to save the configuration.
6. From the "Run" menu, select "Debug" (e.g., "Debug 'my-debug-configuration'") to start the debugger.

## Related documentation

- [Custom environmental variables](../how-to-guides/custom-environmental-variables.md)
- [Remote debug](https://www.jetbrains.com/help/idea/tutorial-remote-debug.html)
