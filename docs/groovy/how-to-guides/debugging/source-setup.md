---
id: source-setup
title: Debug built-from-source Deephaven with IntelliJ IDEA
---

When building Deephaven from source code for development tasks, you can debug Deephaven's internal Java source and the Deephaven code your Groovy scripts call. This is useful for:

- Contributing to Deephaven development.
- Understanding how Deephaven works internally.
- Debugging issues that require stepping into Deephaven's source.
- Testing your own changes to Deephaven.

This guide walks through setting up the remote JVM debugger in [IntelliJ IDEA](https://www.jetbrains.com/idea/) for a source-built Deephaven instance.

Breakpoints in your own Groovy scripts don't bind. [Step 5](#5-use-the-intellij-debugger) explains why and shows how to debug the Deephaven code a script calls instead.

## Prerequisites

This guide assumes you built Deephaven by following the [guide on building Deephaven from source](../../getting-started/launch-build.md). Those steps produce a local clone of the [deephaven-core repository](https://github.com/deephaven/deephaven-core) in a directory called `deephaven-core`.

You also need [IntelliJ IDEA](https://www.jetbrains.com/idea/). Remote JVM Debug is available in every edition of IntelliJ IDEA, including the free one (Community Edition before 2025.3).

## 1. Create an IntelliJ project

Open IntelliJ IDEA. From the welcome screen, click **Open** and navigate to your `deephaven-core` directory.

IntelliJ imports the Gradle project. The first import can take several minutes while IntelliJ resolves dependencies and indexes sources.

![img](../../assets/how-to/debugging/source-1.png)

## 2. Create a remote debug configuration

Go to **Run > Edit Configurations**. Click **+** in the top-left corner and select **Remote JVM Debug**.

Give the configuration a name such as `source-debug`.

Set the following fields:

- **Debugger mode**: **Attach to remote JVM**
- **Host**: `localhost`
- **Port**: `5005`

Port 5005 is the default debug port that the server opens when you start it with `-Pdebug` in [step 3](#3-start-the-server-with-debugging-enabled). If you use a different port there, enter the same port here.

![img](../../assets/how-to/debugging/source-2.png)

Click **Apply** and **OK**.

## 3. Start the server with debugging enabled

Open a terminal in the `deephaven-core` directory and run:

```bash
./gradlew server-jetty-app:run -Pgroovy -Pdebug
```

The `-Pdebug` flag starts the server's JVM with [JDWP](https://docs.oracle.com/en/java/javase/21/docs/specs/jdwp/jdwp-spec.html), the Java Debug Wire Protocol that IntelliJ connects to. It listens on port 5005 by default. To use another port, pass it to the flag, for example `-Pdebug=5006`.

> [!WARNING]
> Anyone who can reach a JDWP port can run code in the JVM. `-Pdebug` opens the port on all of your machine's network interfaces, not just `localhost`. Use it only on a machine and network you trust, and don't enable it on a server other people can reach.

> [!NOTE]
> The Gradle task keeps running and holds the terminal for as long as the server is up. This is expected. Leave this terminal open while you debug.

Early in the output, the JVM reports that the debug port is open:

```text
Listening for transport dt_socket at address: 5005
```

![img](../../assets/how-to/debugging/source-3.png)

By default, the server uses [pre-shared key authentication](../authentication/auth-psk.md) and prints the key to the server log, so enter that key when you open the Deephaven IDE. To skip the key prompt, add the `-Panonymous` flag to the command above. The flag enables [anonymous authentication](../authentication/auth-anon.md), so anyone who can reach port 10000 can use the server, and the server listens on all network interfaces. Use it only on a network you trust. The debugger works with either.

The server is ready when the log shows `Server started on port 10000`. Open the Deephaven IDE at [http://localhost:10000/ide/](http://localhost:10000/ide/).

## 4. Attach the debugger

In IntelliJ IDEA, start the debug configuration by selecting **Run > Debug 'source-debug'** or by clicking the bug icon in the toolbar.

IntelliJ connects to the server on port 5005. The **Debug** tool window shows `Connected to the target VM, address: 'localhost:5005', transport: 'socket'`.

![img](../../assets/how-to/debugging/source-4.png)

## 5. Use the IntelliJ debugger

How you debug depends on whether the code you want to step through is Deephaven's Java source or your own Groovy script:

**Debugging Deephaven's internals**: Open a Java source file from the `deephaven-core` project and click in the left margin to set a breakpoint. Then, in the Deephaven console, run a command that reaches that code. IntelliJ pauses at the line, and you can inspect the call stack and local variables and step through the code.

For example, open `engine/table/src/main/java/io/deephaven/engine/table/impl/QueryTable.java`, set a breakpoint on the first line of `where(Filter filter)`, and run the following in the console. IntelliJ pauses inside [`where`](../../reference/table-operations/filter/where.md).

```groovy order=t
t = emptyTable(10).update("X = ii").where("X > 5")
```

While execution is paused in a command you ran from the console, that command holds the [update graph](../../conceptual/dag.md#update-graph-cycles)'s lock. [Ticking tables](../../conceptual/table-types.md) stop updating, and other console commands wait, until you resume. The tables then catch up.

**Debugging your own Groovy scripts**: Breakpoints in your own Groovy scripts don't bind. That's true for scripts typed into the console and for `.groovy` files loaded with [Application Mode](../application-mode.md). Deephaven compiles each script at runtime into a class with a generated name in the `io.deephaven.dynamic` package, and IntelliJ can't match that class to your file. Instead:

- Set breakpoints in the Deephaven Java methods your script calls, as described under **Debugging Deephaven's internals** above.
- Move the logic you want to step through into a Java class in a module of your `deephaven-core` clone that the server depends on, and set breakpoints there.
- Trace values with `println`. The output appears in the terminal where the server runs. See [Breakpoints in Groovy scripts are not hit](./common-problems.md#breakpoints-in-groovy-scripts-are-not-hit).

IntelliJ IDEA's debugger can step through Java source, inspect variables, evaluate expressions, and navigate the call stack. [JetBrains' debugging guide](https://www.jetbrains.com/help/idea/debugging-code.html) is a good starting point for general debugger usage.

## Troubleshooting

### Debugger won't connect

**Problem**: IntelliJ reports a connection error when starting the debug configuration.

**Solutions**:

- Verify the server started and shows the `Listening for transport dt_socket at address: 5005` message.
- Check that no firewall blocks port 5005.
- Ensure the port in the IntelliJ configuration matches the port used with `-Pdebug`.

### Gradle build fails

**Problem**: The `./gradlew server-jetty-app:run -Pgroovy -Pdebug` command fails with errors.

**Solutions**:

- Verify you successfully built Deephaven from source following the [build guide](../../getting-started/launch-build.md).
- Check that you are running the Gradle command from the `deephaven-core` repository root.
- Ensure the build guide's [prerequisites](../../getting-started/launch-build.md#prerequisites) are installed.
- Try cleaning and re-running: `./gradlew clean`, followed by `./gradlew server-jetty-app:run -Pgroovy -Pdebug`.

### IntelliJ can't find Deephaven source files

**Problem**: When stepping through code, IntelliJ shows "Source code not available" for Deephaven classes.

**Solutions**:

- Verify the IntelliJ project was opened from the same `deephaven-core` directory used to build and run the server.
- Wait for IntelliJ to finish indexing the project after you first open it. The progress bar at the bottom of the window shows indexing progress.
- Ensure the Gradle sync completed without errors. To re-run it, open **View > Tool Windows > Gradle** and click the refresh icon.

### Breakpoints are not hit

**Problem**: Execution runs through a breakpoint without pausing.

**Solutions**:

- Confirm that the debugger is connected. Look for the `Connected to the target VM` line in IntelliJ's **Debug** tool window.
- Set breakpoints in Java source, not in Groovy scripts. Breakpoints in Groovy scripts don't bind, whether you type the script into the console or load it with [Application Mode](../application-mode.md). See [step 5](#5-use-the-intellij-debugger).
- If you use [`updateView`](../../reference/table-operations/select/update-view.md), switch to [`select`](../../reference/table-operations/select/select.md) to force eager evaluation. See [Lazy evaluation of some table operations](./common-problems.md#lazy-evaluation-of-some-table-operations).

## Related documentation

- [Deephaven's live Directed-Acyclic-Graph (DAG)](../../conceptual/dag.md)
- [Deephaven's table types](../../conceptual/table-types.md)
- [Build Deephaven from source code](../../getting-started/launch-build.md)
- [Debug Groovy code in Deephaven](./debugging-landing.md)
- [Common problems when debugging Deephaven](./common-problems.md)
- [Debug Docker-run Deephaven with IntelliJ IDEA](./docker-setup.md)
- [Application Mode](../application-mode.md)
- [Pre-shared key authentication](../authentication/auth-psk.md)
- [Anonymous authentication](../authentication/auth-anon.md)
- [`emptyTable`](../../reference/table-operations/create/emptyTable.md)
- [`select`](../../reference/table-operations/select/select.md)
- [`update`](../../reference/table-operations/select/update.md)
- [`updateView`](../../reference/table-operations/select/update-view.md)
- [`where`](../../reference/table-operations/filter/where.md)
