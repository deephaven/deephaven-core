---
id: docker-setup
title: Debug Docker-run Deephaven with IntelliJ IDEA
---

This guide shows you how to connect IntelliJ IDEA's remote debugger to Deephaven running in a Docker container.

When Deephaven runs in Docker, the engine and user code execute inside a [Docker container](https://www.docker.com/resources/what-container/). Debugging uses [JDWP](https://docs.oracle.com/en/java/javase/21/docs/specs/jdwp/jdwp-spec.html) (Java Debug Wire Protocol), which is built into the JVM. You expose the debug port from the container and connect IntelliJ IDEA to it. To debug a Deephaven build you run from source instead, see the [source debugging setup](./source-setup.md).

This setup lets you attach to the JVM and debug Deephaven's own Java code. Breakpoints in your own Groovy scripts do not fire, so trace them with `println` instead. See [Trace your Groovy scripts with `println`](#trace-your-groovy-scripts-with-println).

You don't need a paid [IntelliJ IDEA](https://www.jetbrains.com/idea/) license for this. Remote JVM Debug is available in every edition of IntelliJ IDEA, including the free one. Before 2025.3, the free edition was IntelliJ IDEA Community Edition.

## 1. Configure Docker for debugging

The Deephaven Docker image passes the contents of the `START_OPTS` environment variable to the JVM as startup options. For more about `START_OPTS`, see [Modify the deployment](../../getting-started/docker-install.md#modify-the-deployment). To enable JDWP, make two changes to your `docker-compose.yml`:

1. Expose the debug port (`5005`) from the container.
2. Add the `-agentlib:jdwp` JVM option to `START_OPTS`.

The following `docker-compose.yml` includes both changes. If you don't have one yet, create a directory with an empty `data` subdirectory and save this file in it.

The `volumes` entry mounts `data` at `/data` in the container. Keep your [Application Mode](../application-mode.md) scripts and data files there.

```yaml
services:
  deephaven:
    image: ghcr.io/deephaven/server-slim:latest
    ports:
      - "127.0.0.1:${DEEPHAVEN_PORT:-10000}:10000"
      - "127.0.0.1:5005:5005"
    volumes:
      - ./data:/data
    environment:
      - START_OPTS=-Xmx4g -DAuthHandlers=io.deephaven.auth.AnonymousAuthenticationHandler -agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=*:5005
```

To debug Deephaven's own Java code, replace `latest` with a specific release tag, such as `42.7`, so you know which source version to check out in step 2.

To see which version your local `latest` image is, run `docker image inspect ghcr.io/deephaven/server-slim:latest --format '{{ index .Config.Labels "org.opencontainers.image.version" }}'`. Run `docker compose pull` first if you want the current release.

The `-agentlib:jdwp` option sets these sub-options:

- `transport=dt_socket` — the debugger connects to the JVM over a TCP socket.
- `server=y` — the JVM listens for debugger connections instead of connecting out to a debugger.
- `suspend=n` — the JVM starts immediately without waiting for a debugger to connect. Set to `y` to pause startup until a debugger attaches, which is useful for debugging initialization issues.
- `address=*:5005` — the JVM listens on port `5005` on all network interfaces. Inside Docker, the `*:` prefix lets the JVM accept connections from outside the container.

> [!WARNING]
> Anyone who can reach a JDWP port can run code in the JVM. The `127.0.0.1:` prefix in the `ports` entry publishes port `5005` only on your own machine. Don't remove it, and don't enable JDWP on a server other people can reach.

> [!NOTE]
> `START_OPTS` must appear only once in the `environment` section, but it can hold multiple JVM options.

> [!NOTE]
> The `-DAuthHandlers=io.deephaven.auth.AnonymousAuthenticationHandler` setting above enables [anonymous authentication](../authentication/auth-anon.md), which lets any client connect without a password. With it enabled, you can open the Deephaven IDE without a [pre-shared key](../authentication/auth-psk.md). It is a convenience, not a requirement for the debugger.
>
> The `127.0.0.1:` prefix in the `ports` entry publishes port `10000` only on your own machine, so other computers on your network can't reach the server. Keep the `127.0.0.1:` prefix while [anonymous authentication](../authentication/auth-anon.md) is enabled.

## 2. Set up IntelliJ for remote debugging

To set breakpoints in Deephaven's Java code, IntelliJ needs the matching Deephaven source. Clone the [deephaven-core repository](https://github.com/deephaven/deephaven-core), check out the tag for your image version (for example, `git checkout v42.7` for `server-slim:42.7`), and open the clone as your IntelliJ project.

If you only need thread dumps and heap state, any project works. Skip to the debug configuration below.

Next, create the debug configuration: go to **Run > Edit Configurations**, click **+** in the top-left corner, and select **Remote JVM Debug**.

Give the configuration a name such as `docker-debug`.

Set the following fields:

- **Debugger mode**: **Attach to remote JVM**
- **Host**: `localhost`
- **Port**: `5005`

> [!NOTE]
> IntelliJ also displays a **Command line arguments for remote JVM** field. It shows the `-agentlib:jdwp` string that the target JVM needs. You already added this option to `START_OPTS` in step 1, so you don't need to copy it.

![img](../../assets/how-to/debugging/docker-1.png)

Click **Apply** and **OK**.

## 3. Start Deephaven

Start the Deephaven container from a terminal in the directory containing your `docker-compose.yml`:

```shell
docker compose up
```

Early in the startup output, you should see:

```text
Listening for transport dt_socket at address: 5005
```

This confirms the JVM is ready for a debugger connection. You can attach before the server finishes starting.

![img](../../assets/how-to/debugging/docker-2.png)

## 4. Attach the debugger

In IntelliJ IDEA, start the debug configuration by selecting **Run > Debug 'docker-debug'** or by clicking the bug icon in the toolbar.

IntelliJ connects to port `5005`, and the **Debug** tool window's console shows:

```text
Connected to the target VM, address: 'localhost:5005', transport: 'socket'
```

![img](../../assets/how-to/debugging/docker-3.png)

## 5. Use the IntelliJ debugger

Once connected, IntelliJ has access to the running JVM. You can inspect thread dumps and heap state, and set breakpoints in Deephaven's own compiled Java code. Breakpoints in Deephaven's Java code need the matching sources you opened as the project in step 2.

When a breakpoint in Deephaven's Java code fires, IntelliJ IDEA's debugger can step through the code and inspect variables. [JetBrains' debugging guide](https://www.jetbrains.com/help/idea/debugging-code.html) is a good reference for general debugger usage.

> [!NOTE]
> **Breakpoints in user-defined Groovy scripts do not work, whether Deephaven runs in Docker or from source.** Deephaven compiles [Application Mode](../application-mode.md) scripts and console code dynamically, and the compiled code doesn't match your source file in two ways:
>
> - Deephaven prepends a `package io.deephaven.dynamic;` declaration, which shifts every line number by one.
> - The compiled class gets a generated name. An [Application Mode](../application-mode.md) script's class name comes from its file path, and console code compiles to a numbered class such as `Script_1`.
>
> IntelliJ cannot map the compiled code back to your `.groovy` files, so breakpoints set in them are never hit.
>
> For the generated class names in detail and a worked example, see [Breakpoints in Groovy scripts are not hit](./common-problems.md#breakpoints-in-groovy-scripts-are-not-hit).

## Trace your Groovy scripts with `println`

IntelliJ can't stop at breakpoints in your own Groovy scripts, as the note in [step 5](#5-use-the-intellij-debugger) explains, so trace their logic with `println` statements instead. This needs only the running container from [step 3](#3-start-deephaven), not the IntelliJ debugger.

The `println` output appears in the container logs and in the **Log** panel of the Deephaven IDE.

Here is an example that traces calls through a function. Run it in the console of the Deephaven IDE at [http://localhost:10000/ide/](http://localhost:10000/ide/) (replace `10000` with your `DEEPHAVEN_PORT` value if you set one):

```groovy order=null
addOne = { long x ->
    println "[DEBUG] addOne called: x=$x"
    def result = x + 1
    println "[DEBUG] addOne returning: $result"
    result
}

t = emptyTable(10).update("X = ii")
tTraced = t.select("X", "Y = addOne(X)")
```

[`select`](../../reference/table-operations/select/select.md) calls `addOne` right away for every row, so the trace appears as soon as the code runs. [`updateView`](../../reference/table-operations/select/update-view.md) would defer the calls. See [Lazy evaluation of some table operations](./common-problems.md#lazy-evaluation-of-some-table-operations).

Follow the output while the container runs:

```shell
docker compose logs -f
```

The log shows each call:

```text
deephaven-1  | [DEBUG] addOne called: x=0
deephaven-1  | [DEBUG] addOne returning: 1
deephaven-1  | [DEBUG] addOne called: x=1
deephaven-1  | [DEBUG] addOne returning: 2
...
```

## Troubleshooting

### Debugger won't connect

**Problem**: IntelliJ reports a connection error when starting the debug configuration.

**Solutions**:

- Verify the Deephaven container is running and shows the `Listening for transport dt_socket` message.
- Check that your `docker-compose.yml` exposes port `5005` and that no firewall blocks it.
- Ensure the port in the IntelliJ configuration matches the port in `START_OPTS`.

### Breakpoints are not hit

**Problem**: Execution runs through a breakpoint without pausing.

**Cause**: The breakpoint is in one of two places:

- A user-defined Groovy script, either an [Application Mode](../application-mode.md) script or console code. These breakpoints never fire. The note in [step 5](#5-use-the-intellij-debugger) explains why, and [Breakpoints in Groovy scripts are not hit](./common-problems.md#breakpoints-in-groovy-scripts-are-not-hit) has more detail.
- Deephaven's Java code, when the sources IntelliJ has open don't match the server version.

**Solutions**:

- Use `println` statements to trace execution, as shown in [Trace your Groovy scripts with `println`](#trace-your-groovy-scripts-with-println). Output appears in `docker compose logs -f` and in the IDE's **Log** panel.
- If you set breakpoints in Deephaven's own Java code, open a deephaven-core clone checked out at the tag for your image version (for example, `v42.7` for `server-slim:42.7`), as described in [step 2](#2-set-up-intellij-for-remote-debugging). IntelliJ needs these sources to match the server version.

### Debugger disconnects after attaching

**Problem**: The debugger connects but the connection immediately drops.

**Solutions**:

- Check the IntelliJ debug console for error messages indicating the cause.
- Try increasing JVM memory if you see out-of-memory errors. The `-Xmx4g` option in the `START_OPTS` environment variable of `docker-compose.yml` sets the maximum heap size. See [Add more memory](../../getting-started/docker-install.md#add-more-memory).

## Related documentation

- [Install Deephaven with Docker](../../getting-started/docker-install.md)
- [Debug Groovy code in Deephaven](./debugging-landing.md)
- [Common problems when debugging Deephaven](./common-problems.md)
- [Debug built-from-source Deephaven with IntelliJ IDEA](./source-setup.md)
- [Enable anonymous authentication](../authentication/auth-anon.md)
- [Pre-shared key authentication](../authentication/auth-psk.md)
- [Application Mode](../application-mode.md)
- [`emptyTable`](../../reference/table-operations/create/emptyTable.md)
- [`select`](../../reference/table-operations/select/select.md)
- [`update`](../../reference/table-operations/select/update.md)
- [`updateView`](../../reference/table-operations/select/update-view.md)
