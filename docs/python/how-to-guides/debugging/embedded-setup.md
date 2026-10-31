---
id: embedded-setup
title: Debug an embedded Deephaven server with PyCharm
---

The [Deephaven Python embedded server](../../getting-started/pip-install.md) (the `deephaven-server` package) starts the Deephaven engine directly inside your own Python process, rather than running it as a separate server you connect to. This guide shows you how to build the embedded server from source and debug it with [PyCharm](https://www.jetbrains.com/pycharm/).

> [!NOTE]
> If you only need to debug a pip-installed release of `deephaven-server`, see the [pip debugging guide](./pip-setup.md) instead. This guide is for developers who build the embedded server from a local `deephaven-core` clone, for example to step into Deephaven's Python source or test local changes.

## How this differs from other debugging setups

With the embedded server, your Python process embeds the JVM, so your script, the Deephaven engine, and PyCharm's debugger backend all run in one process. It's the same process PyCharm already runs when you launch a script. The setups compare as follows:

- **[Docker](./docker-setup.md) and [Gradle source build](./source-setup.md)**: The Deephaven server runs as its own process, in a container or from `./gradlew server-jetty-app:run`, and its JVM embeds a Python interpreter. To debug it, you run PyCharm's remote debugging server, which is a paid feature. In the Deephaven console, you call `settrace` from [`pydevd`](https://pypi.org/project/pydevd/), the debugger library PyCharm uses.
- **Embedded server**: PyCharm launches the single Python process that runs your script and the engine. You debug it locally, the same way you'd [debug a pip installation locally](./pip-setup.md#local-debugging-with-pycharm), with no remote debugging server. For code you type into the console, you still call `pydevd.settrace`, but with no host or port ([step 6](#6-debug-code-from-the-deephaven-web-console)).

The build and project setup are similar to the [Gradle source-build debugging guide](./source-setup.md): you build wheels from a local clone into a virtual environment, then open the clone as a PyCharm project.

## Prerequisites

This guide assumes you have a local clone of [Deephaven Core](https://github.com/deephaven/deephaven-core), as described in the [guide on building Deephaven from source](../../getting-started/launch-build.md). On Windows, the clone, the virtual environment, and the build must live inside WSL 2, so run every command in this guide from your WSL distribution.

The embedded server starts a JVM inside your Python process, so set the `JAVA_HOME` environment variable to your JDK 21 installation before you run the launch script you write in [step 3](#3-write-a-launch-script). [Step 4](#4-create-a-run-configuration) shows how to set it in PyCharm if PyCharm doesn't pick it up.

Building the wheels requires [Docker](https://docs.docker.com/get-docker/), because the wheel build runs inside a container. Make sure Docker is installed and the Docker daemon is running before you run the `./gradlew` commands in [step 1](#1-build-and-install-the-wheels). See the [Docker prerequisite](../../getting-started/launch-build.md#docker) in the guide on building Deephaven from source for the required version.

You also need PyCharm. On macOS and Linux, the free tier of PyCharm works, because this setup uses only local debugging. Before 2025.1, the free tier was PyCharm Community Edition. On Windows, this setup uses a WSL interpreter, which requires PyCharm Pro.

> [!WARNING]
> Use PyCharm 2024 or later. Debugging Deephaven with PyCharm 2023 has known problems.

## 1. Build and install the wheels

From the root of the `deephaven-core` clone, create and activate a virtual environment:

```bash
python3 -m venv embedded-dh-venv
source embedded-dh-venv/bin/activate
```

The rest of this guide calls this virtual environment `embedded-dh-venv`. If you use a different name, substitute it.

Then, build the Python server wheel and the embedded server wheel:

```bash
./gradlew :py-server:assemble
./gradlew :py-embedded-server:assemble
```

Install both wheels into `embedded-dh-venv` with one command. The embedded server wheel, `deephaven_server`, requires exactly the same version of `deephaven-core`, which is the `py-server` wheel you just built. When you pass both files to one `pip install`, pip takes `deephaven-core` from your local build instead of PyPI:

```bash
pip install --force-reinstall \
  py/server/build/wheel/deephaven_core-<version>-py3-none-any.whl \
  py/embedded-server/build/wheel/deephaven_server-<version>-py3-none-any.whl
```

Replace `<version>` with the version in the built wheel filenames. To see them, run `ls py/server/build/wheel/ py/embedded-server/build/wheel/`. Both wheels have the same version.

Also install [`pydevd`](https://pypi.org/project/pydevd/). [Step 6](#6-debug-code-from-the-deephaven-web-console) uses it to set programmatic breakpoints:

```bash
pip install pydevd
```

## 2. Create a PyCharm project

Create a PyCharm project from the `deephaven-core` clone used to build the wheels in [step 1](#1-build-and-install-the-wheels). From PyCharm's launch window, click **Open** and navigate to the `deephaven-core` directory. Opening the clone lets you edit Deephaven's source alongside your launch script and rebuild the wheels from the same project.

Set the project interpreter to `embedded-dh-venv`. Open **PyCharm** > **Settings** on macOS, or **File** > **Settings** on Windows and Linux. Then go to **Project: deephaven-core** > **Python Interpreter**, and select the interpreter in `<deephaven-core>/embedded-dh-venv/bin/python` if it isn't already selected.

On Windows, add the WSL 2 interpreter with **Add New Interpreter** > **On WSL**, and select `embedded-dh-venv/bin/python` inside your WSL distribution.

## 3. Write a launch script

Create a script, such as `script.py`, that starts the embedded server:

```python skip-test
import time

from deephaven_server import Server

s = Server(
    host="localhost",
    port=10000,
    jvm_args=[
        "-Xmx4g",
        "-DAuthHandlers=io.deephaven.auth.AnonymousAuthenticationHandler",
        "-Dprocess.info.system-info.enabled=false",
    ],
)
s.start()

from deephaven import empty_table

t = empty_table(10).update("X = ii")

# Keep the process (and the embedded server) alive
while True:
    time.sleep(1)
```

The `jvm_args` list passes these options to the embedded JVM:

- `-Xmx4g` sets the JVM's maximum heap to 4 GB. Raise or lower it to fit your data and your machine.
- `-DAuthHandlers=io.deephaven.auth.AnonymousAuthenticationHandler` enables [anonymous authentication](../authentication/auth-anon.md), so you can open the Deephaven web IDE without a [pre-shared key](../authentication/auth-psk.md). Debugging doesn't depend on it.
- `-Dprocess.info.system-info.enabled=false` turns off collection of operating system and hardware information at startup. Debugging doesn't depend on it, but on M2 Macs keep it, as the [pip install guide](../../getting-started/pip-install.md#start-a-deephaven-server) recommends.

The [`empty_table`](../../reference/table-operations/create/emptyTable.md) line stands in for your own code. Set a breakpoint on it to check that the debugger stops in the launch script. Import from `deephaven` only after you create the `Server`, because the `deephaven` package needs the JVM that `Server` starts.

Anonymous authentication provides no security, so use it only on a server bound to `localhost`, as `host="localhost"` does here.

Without the `AuthHandlers` argument, the server uses [pre-shared key authentication](../authentication/auth-psk.md) with a random key. A server started from a Python script doesn't print that key to the console, so you can't look it up. To use a [pre-shared key](../authentication/auth-psk.md) you choose, replace the `AuthHandlers` argument with `-Dauthentication.psk=<your-key>`. Enter that key when you open the web IDE in [step 6](#6-debug-code-from-the-deephaven-web-console).

The `while True` loop at the end of the launch script keeps the Python process running, which keeps the embedded server and the debugger session alive. Without the loop, the process exits when the script reaches its last line.

The loop also matters when you debug a function called from a [ticking table](../../conceptual/table-types.md). Deephaven runs [ticking-table](../../conceptual/table-types.md) updates on its own threads, so the process must still be running when an update reaches your code. See [Ticking tables when your script starts the server](./common-problems.md#ticking-tables-when-your-script-starts-the-server).

## 4. Create a run configuration

1. Go to **Run** > **Edit Configurations**.
2. Click the **+** button and select **Python**.
3. Name the configuration (e.g., "Embedded Server Debug").
4. Set **Script path** to the launch script from [step 3](#3-write-a-launch-script) (e.g., `script.py`), and set the interpreter to `embedded-dh-venv`.
5. If PyCharm doesn't pick up `JAVA_HOME` from your environment, add `JAVA_HOME` under **Environment variables**, set to your JDK 21 installation.
6. Click **OK** to save the configuration.

## 5. Debug the launch script

Set breakpoints in the launch script before you start the debugger, because its code runs as soon as the process starts. Because everything runs in the interpreter PyCharm is debugging, PyCharm stops at breakpoints in your own code and in Deephaven's Python source.

When you install the wheels, pip copies Deephaven's Python modules into the `site-packages` directory of `embedded-dh-venv`. That installed copy is the code that runs, so set breakpoints in Deephaven's source there. Open it under **External Libraries** in PyCharm's **Project** tool window, not `py/server` in the clone.

Then select the run configuration you created in [step 4](#4-create-a-run-configuration), such as "Embedded Server Debug", and click the **Debug** button. PyCharm launches the script in a Python process and attaches its debugger to that process before any Deephaven or JVM code runs.

If you change `py/server` or Deephaven's Java code, rebuild and reinstall the wheels ([step 1](#1-build-and-install-the-wheels)) before you debug again. The embedded server wheel contains the server's Java JARs, so Java changes take effect only after you rebuild it.

## 6. Debug code from the Deephaven web console

Once the server starts, open the Deephaven web IDE at `http://localhost:10000/ide/` and run code in its console. If you set a [pre-shared key](../authentication/auth-psk.md) in [step 3](#3-write-a-launch-script), enter it when the web IDE asks for it.

Because the embedded server runs in the same process as your launch script, code executed in the console runs in that same interpreter. Console commands run on Deephaven's server threads rather than on your script's main thread. Deephaven registers those threads with the debugger, so PyCharm stops at the breakpoints you set.

Code you type into the console, such as a [user-defined function](../python-functions.md) that a [query string](../query-string-overview.md) calls, isn't in a file open in PyCharm, so you can't click in the margin to set a breakpoint in it. Call `pydevd.settrace` in that code as a programmatic breakpoint instead:

```python skip-test
import pydevd
from deephaven import empty_table


def udf(x) -> int:
    # Execution pauses here, like a breakpoint
    pydevd.settrace()
    y = x + 1
    return y


t = empty_table(10).update("X = ii")
t_new = t.update("Y = udf(X)")
```

The console and your launch script share the same debugged process, so PyCharm's debugger is already attached. That's why `pydevd.settrace` doesn't need a host or port here.

PyCharm pauses each time `udf` runs, once for each of the table's 10 rows. Click **Resume Program** to move to the next row.

While execution is paused in a command you ran from the console, that command holds the [update graph](../../conceptual/dag.md#update-graph-cycles)'s lock. Ticking tables stop updating, and other console commands wait, until you resume. The tables then catch up.

Deephaven has some debugging behaviors of its own, such as [lazily evaluated table operations](./common-problems.md#lazy-evaluation-of-some-table-operations) and [ticking-table](../../conceptual/table-types.md) updates on other threads. Check out [Common problems](./common-problems.md) before you dig into a breakpoint that won't hit.

## Debug Deephaven's Java engine

PyCharm's debugger reaches only Python code. Deephaven's query engine is Java, so PyCharm can't step into [table operations](../../conceptual/table-operations-overview.md) or the [update graph](../../conceptual/dag.md#update-graph-cycles). To debug the Java engine in the same process, add a [JDWP](https://docs.oracle.com/en/java/javase/21/docs/specs/jpda/conninv.html) agent to `jvm_args` in your [launch script](#3-write-a-launch-script):

```python skip-test
s = Server(
    host="localhost",
    port=10000,
    jvm_args=[
        "-Xmx4g",
        "-DAuthHandlers=io.deephaven.auth.AnonymousAuthenticationHandler",
        "-Dprocess.info.system-info.enabled=false",
        "-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=127.0.0.1:5005",
    ],
)
```

The `address=127.0.0.1:5005` setting makes the JVM listen for a debugger on port 5005 of your own machine only. Then attach a JVM debugger, such as an IntelliJ IDEA **Remote JVM Debug** configuration, to port 5005. Remote JVM Debug is available in every edition of IntelliJ IDEA, including the free one.

## Troubleshooting

### Gradle build fails

**Problem**: `./gradlew :py-embedded-server:assemble` fails with errors.

**Solutions**:

- Make sure Docker is installed and the Docker daemon is running. The wheel builds run in a container.
- Verify you can build the [full source-based server](../../getting-started/launch-build.md) first. The embedded server wheel depends on the same build infrastructure.
- Check that you're running the Gradle command from the `deephaven-core` repository root.

### `pip install` fails or pulls the wrong version

**Problem**: Installing the `deephaven_server` wheel tries to download `deephaven-core` from PyPI instead of using your local build, or fails because no matching version is published.

**Solutions**:

- Pass the locally built `deephaven_core` wheel from `py/server/build/wheel/` and the `deephaven_server` wheel to the same `pip install` command, as shown in [step 1](#1-build-and-install-the-wheels).

### Breakpoints in Deephaven source don't bind

**Problem**: Breakpoints set in files under `py/server/deephaven` appear grayed out or never hit.

**Solutions**:

- Set breakpoints in Deephaven's Python source in the copy installed in `embedded-dh-venv`, not in `py/server` in the clone. Python runs the installed wheel from `site-packages`, not the source in the clone. PyCharm lists the installed copy under **External Libraries** in its **Project** tool window. See [step 5](#5-debug-the-launch-script).

### PyCharm can't find Deephaven source files

**Problem**: When debugging, PyCharm shows "Source code not available" or can't find files.

**Solutions**:

- Check that the Python interpreter is set to `embedded-dh-venv`.
- If the breakpoints you set in Deephaven's source never hit, see [Breakpoints in Deephaven source don't bind](#breakpoints-in-deephaven-source-dont-bind).

### Console code isn't hitting breakpoints

**Problem**: Breakpoints work in your launch script but not in code run from the console.

**Solutions**:

- Make sure you started the launch script with **Debug**, not **Run**. Under **Run**, no debugger is attached to the process.
- Use `pydevd.settrace` directly in the code you want to debug, as shown in [step 6](#6-debug-code-from-the-deephaven-web-console).
- If the code is a function that a query calls, see [Breakpoints in a function called from a query never hit](#breakpoints-in-a-function-called-from-a-query-never-hit).

### Breakpoints in a function called from a query never hit

**Problem**: A breakpoint in a function that a [table operation](../../conceptual/table-operations-overview.md) calls is never reached.

**Solutions**:

- If the operation is [lazily evaluated](./common-problems.md#lazy-evaluation-of-some-table-operations), such as [`update_view`](../../reference/table-operations/select/update-view.md), force evaluation with [`select`](../../reference/table-operations/select/select.md).
- If the table is [ticking](../../conceptual/table-types.md), make sure the process is still running when an update reaches your code. The `while True` loop in the [launch script](#3-write-a-launch-script) does this. See [Ticking tables when your script starts the server](./common-problems.md#ticking-tables-when-your-script-starts-the-server).

## Related documentation

- [Deephaven's live Directed-Acyclic-Graph (DAG)](../../conceptual/dag.md)
- [Deephaven's table types](../../conceptual/table-types.md)
- [Table operations overview](../../conceptual/table-operations-overview.md)
- [Debug Python code in Deephaven](./debugging-landing.md)
- [Install Deephaven with pip](../../getting-started/pip-install.md)
- [Build and run Deephaven from source code](../../getting-started/launch-build.md)
- [Debug Docker-run Deephaven with PyCharm](./docker-setup.md)
- [Debug pip-installed Deephaven](./pip-setup.md)
- [Debug built-from-source Deephaven with PyCharm](./source-setup.md)
- [Common problems when debugging Deephaven](./common-problems.md)
- [Query strings](../query-string-overview.md)
- [Python functions in query strings](../python-functions.md)
- [Anonymous authentication](../authentication/auth-anon.md)
- [Pre-shared key authentication](../authentication/auth-psk.md)
- [`empty_table`](../../reference/table-operations/create/emptyTable.md)
- [`update`](../../reference/table-operations/select/update.md)
- [`update_view`](../../reference/table-operations/select/update-view.md)
- [`select`](../../reference/table-operations/select/select.md)
