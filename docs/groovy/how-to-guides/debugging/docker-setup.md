---
id: docker-setup
title: Debug Docker-run Deephaven with IntelliJ IDEA
sidebar_label: Docker
---

This guide shows you how to connect IntelliJ IDEA's remote debugger to Deephaven running in a Docker container.

When Deephaven runs in Docker, the engine and user code execute inside a [Docker container](https://www.docker.com/resources/what-container/). Debugging uses [JDWP](https://docs.oracle.com/en/java/javase/17/docs/specs/jdwp/jdwp-spec.html) (Java Debug Wire Protocol), which is built into the JVM. You expose the debug port from the container and connect IntelliJ IDEA to it.

Both [IntelliJ IDEA Community and Ultimate](https://www.jetbrains.com/idea/) editions support remote JVM debugging.

## 1. Configure Docker for debugging

To enable JDWP, two changes are needed in your `docker-compose.yml`:

1. Expose the debug port (5005) from the container.
2. Add the `-agentlib:jdwp` JVM option to `START_OPTS`.

The following `docker-compose.yml` includes both changes:

```yaml
services:
  deephaven:
    image: ghcr.io/deephaven/server-slim:latest
    ports:
      - "${DEEPHAVEN_PORT:-10000}:10000"
      - "5005:5005"
    volumes:
      - ./data:/data
    environment:
      - START_OPTS=-Xmx4g -DAuthHandlers=io.deephaven.auth.AnonymousAuthenticationHandler -agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=*:5005
```

> [!NOTE]
> `START_OPTS` should only appear once in the `environment` section, but can include multiple JVM options.

> [!WARNING]
> Anonymous authentication is required for this setup. The `DAuthHandlers` setting above enables it.

The `-agentlib:jdwp` sub-options used here:

- `server=y` — the JVM listens for debugger connections (rather than connecting to a debugger).
- `suspend=n` — the JVM starts immediately without waiting for a debugger to connect. Set to `y` to pause startup until a debugger attaches, which is useful for debugging initialization issues.
- `address=*:5005` — listen on port 5005 on all network interfaces. The `*:` prefix is required when the JVM is inside Docker, so that connections from outside the container are accepted.

## 2. Create a remote debug configuration in IntelliJ

Open IntelliJ IDEA and go to **Run > Edit Configurations**. Click **+** in the top-left corner and select **Remote JVM Debug**.

Give the configuration a name such as `docker-debug`.

Set the following fields:

- **Debugger mode**: Attach to remote JVM
- **Host**: `localhost`
- **Port**: `5005`

> [!NOTE]
> IntelliJ also displays a **"Command line arguments for remote JVM"** field showing the `-agentlib:jdwp` string. This is informational — it shows what the target JVM needs. You already added this to `START_OPTS` in step 1, so no further action is needed.

![img](../../assets/how-to/debugging/docker-1.png)

Click **Apply** and **OK**.

## 3. Start Deephaven

Start the Deephaven container from a terminal in the directory containing your `docker-compose.yml`:

```shell
docker compose up
```

Wait for the server to finish starting. You should see output similar to:

```text
Listening for transport dt_socket at address: 5005
```

This confirms the JVM is ready for a debugger connection.

![img](../../assets/how-to/debugging/docker-2.png)

## 4. Attach the debugger

In IntelliJ IDEA, start the debug configuration by selecting **Run > Debug 'docker-debug'**, or click the bug icon in the toolbar.

IntelliJ will connect to port 5005. The debugger status bar at the bottom of the IDE will show that it is connected.

![img](../../assets/how-to/debugging/docker-3.png)

## 5. Use the IntelliJ debugger

Set a breakpoint inside any Groovy method by clicking in the left margin of the editor. When Deephaven calls that method, execution will pause and the debugger panel will become active.

Here's an example script to test your setup. Enter it in the Deephaven console after attaching the debugger, with a breakpoint set on the `return` line of `addOne`:

```groovy
def addOne(int x) {
    return x + 1  // Set a breakpoint here
}

t = emptyTable(10).update("X = ii")
tResult = t.select("X", "Y = addOne(X)")
```

> [!NOTE]
> Use `select` rather than `updateView` when testing breakpoints in methods. `updateView` uses lazy evaluation and may not trigger the breakpoint. See [Common problems](./common-problems.md) for details.

IntelliJ IDEA's debugger can step through Groovy and Java source code, inspect variables, and evaluate expressions. [JetBrains' debugging guide](https://www.jetbrains.com/help/idea/debugging-code.html) is a good reference for general debugger usage.

## Troubleshooting

### Debugger won't connect

**Problem**: IntelliJ reports a connection error when starting the debug configuration.

**Solutions**:

- Verify the Deephaven container is running and shows the `Listening for transport dt_socket` message.
- Check that port 5005 is exposed in your `docker-compose.yml` and is not blocked by a firewall.
- Ensure the port in the IntelliJ configuration matches the port in `START_OPTS`.

### Breakpoints are not hit

**Problem**: Execution runs through a breakpoint without pausing.

**Solutions**:

- Confirm the debugger is connected (check the IntelliJ status bar).
- If using `updateView`, switch to `select` to force eager evaluation. See [Common problems](./common-problems.md).
- Verify the source file open in IntelliJ matches the code running in the container.

### Debugger disconnects after attaching

**Problem**: Connection is established but immediately drops.

**Solutions**:

- Ensure anonymous authentication is enabled via `DAuthHandlers` in `START_OPTS` (see step 1).
- Check the IntelliJ debug console for error messages indicating the cause.
- Try increasing JVM memory if you see out-of-memory errors (adjust `-Xmx4g` in `START_OPTS`).

## Related documentation

- [Install Deephaven with Docker](../../getting-started/docker-install.md)
- [Attach a JVM debugger](../attach-debugger.md)
- [Common problems with debugging](./common-problems.md)
- [Source debugging setup](./source-setup.md)
