---
id: embedded-setup
title: Debug an embedded Deephaven server with PyCharm
sidebar_label: Embedded server
---

The [Deephaven Python embedded server](../../getting-started/pip-install.md) (the `deephaven-server` package) starts the Deephaven engine directly inside your own Python process, rather than running it as a separate server you connect to. This guide shows you how to build the embedded server from source and debug it with [PyCharm](https://www.jetbrains.com/pycharm/).

> [!NOTE]
> If you only need to debug a pip-installed release of `deephaven-server`, see the [pip debugging guide](./pip-setup.md) instead. This guide is for developers who build the embedded server from a local `deephaven-core` clone, for example to step into Deephaven's own source code or test local changes.

## How this differs from other debugging setups

With [Docker](./docker-setup.md) and a [Gradle source build](./source-setup.md), the Deephaven server is its own process (in a container, or started with `./gradlew server-jetty-app:run`). The JVM in that process embeds a Python interpreter to run your scripts and queries. Because your IDE and the server are separate processes, debugging requires PyCharm Professional's remote debugging server and a `pydevd.settrace()` call from the Deephaven console.

The embedded server works the other way around: your Python process embeds the JVM. There is only one process, and it's the same one your IDE already runs when you launch a script. This means you can debug the embedded server locally, the same way you'd debug the [pip installation](./pip-setup.md) — no remote debugging server or `pydevd.settrace()` is required to debug your own launch script.

The build and project setup, however, are the same as the [source build guide](./source-setup.md): you need a local `deephaven-core` clone, a matching virtual environment built from that source, and a PyCharm project opened on the clone so that breakpoints resolve against the real source.

## Setup

This guide assumes you have a local clone of [Deephaven Core](https://github.com/deephaven/deephaven-core), as described in the [guide on building Deephaven from source](../../getting-started/launch-build.md).

## 1. Build the embedded server wheel

From the root of the `deephaven-core` clone, create and activate a virtual environment:

```bash
python3 -m venv embedded-dh-venv
source embedded-dh-venv/bin/activate
```

This guide assumes that virtual environment is called `embedded-dh-venv`.

Then, build the Python server wheel and the embedded server wheel:

```bash
./gradlew :py-server:assemble
./gradlew :py-embedded-server:assemble
```

Install both wheels into `embedded-dh-venv`. The embedded server wheel depends on an exact-matching version of `deephaven-core`, so install the `py-server` wheel first to satisfy that dependency locally, rather than pulling a release from PyPI:

```bash
pip install --force py/server/build/wheel/deephaven_core-<version>-py3-none-any.whl
pip install --force py/embedded-server/build/wheel/deephaven_server-<version>-py3-none-any.whl
```

Also install `pydevd`, which is used later for setting programmatic breakpoints:

```bash
pip install pydevd
```

## 2. Create PyCharm project

Create a PyCharm project from the `deephaven-core` clone used to build the wheels in step 1. From PyCharm's launch window, click **Open** and navigate to the `deephaven-core` directory.

Confirm the project interpreter is set to `embedded-dh-venv` by going to **PyCharm** > **Settings** > **Project: deephaven-core** > **Python Interpreter**.

> [!WARNING]
> Debugging Deephaven with PyCharm has only been shown to work for PyCharm major version **2024** or higher. Some issues have been discovered with using PyCharm 2023, and there is no plan to fix these issues.

## 3. Write and debug a launch script

Create a script, such as `script.py`, that starts the embedded server:

```python skip-test
from deephaven_server import Server

s = Server(
    host="localhost",
    port=10000,
    jvm_args=[
        "-Xmx16g",
        "-DAuthHandlers=io.deephaven.auth.AnonymousAuthenticationHandler",
        "-Dprocess.info.system-info.enabled=false",
    ],
)
s.start()

from deephaven import empty_table

t = empty_table(10).update("X = ii")
```

> [!NOTE]
> The `AuthHandlers` argument enables anonymous authentication, which makes it easier to open the Deephaven web IDE without dealing with a pre-shared key. It is not required to debug your own launch script, but it's recommended if you also plan to debug code executed from the web console in step 4.

Right-click in the editor and select **Debug 'script'**. PyCharm attaches its debugger directly to this process before any Deephaven or JVM code runs. Set breakpoints in your own code or in Deephaven's Python source under `py/server`, and they'll be hit normally, since it's all running in the same interpreter PyCharm is already debugging.

## 4. Debug code from the Deephaven web console

Once the server starts, you can also interact with it through the web IDE at `http://localhost:10000/ide/`. Because the embedded server runs in the same process as your launch script, code executed in the web console runs in that same interpreter, and breakpoints you've already set should still be hit.

There are two things to keep in mind:

- **Keep the main thread alive.** As described in [Common problems](./common-problems.md#ticking-tables-and-the-main-thread), Deephaven only reliably notifies Python debuggers of activity on the main thread. If your script exits immediately after `s.start()`, the process — and the debugger session — will shut down before you can interact with the console. Add a blocking call, such as `time.sleep`, at the end of your script to keep it alive:

  ```python skip-test
  import time

  while True:
      time.sleep(1)
  ```

- **Use `pydevd.settrace()` as a programmatic breakpoint.** This is especially useful for pausing execution inside a query string or user-defined function, where clicking in the margin isn't practical:

  ```python skip-test
  import pydevd
  from deephaven import empty_table


  def udf(x) -> int:
      # Acts like a breakpoint - execution will pause here
      pydevd.settrace()
      y = x + 1
      return y


  t = empty_table(10).update("X = ii")
  t_new = t.update("Y = udf(X)")
  ```

  Because the console and your launch script share the same debugged process, `pydevd.settrace()` doesn't need a host or port — PyCharm's debugger is already attached.

**There are some Deephaven-specific things to consider when debugging.** Check out [Common problems](./common-problems.md) for issues specific to debugging Deephaven's table operations and ticking tables.

## Troubleshooting

### Gradle build fails

**Problem**: `./gradlew :py-embedded-server:assemble` fails with errors.

**Solutions**:

- Verify you can build the [full source-based server](../../getting-started/launch-build.md) first; the embedded server wheel depends on the same build infrastructure.
- Check that you're running the Gradle command from the `deephaven-core` repository root.
- Try building the `py-server` wheel first: `./gradlew :py-server:assemble`.

### `pip install` fails or pulls the wrong version

**Problem**: Installing the `deephaven_server` wheel tries to download `deephaven-core` from PyPI instead of using your local build, or fails because no matching version is published.

**Solutions**:

- Install the locally-built `deephaven_core` wheel from `py/server/build/wheel/` _before_ installing the `deephaven_server` wheel.
- Use `pip install --force` to make sure the local wheels take precedence over anything already installed.

### PyCharm can't find Deephaven source files

**Problem**: When debugging, PyCharm shows "Source code not available" or can't find files.

**Solutions**:

- Verify the PyCharm project was created from the same `deephaven-core` directory used to build the wheels.
- Check that the Python interpreter is set to `embedded-dh-venv`.

### Console code isn't hitting breakpoints

**Problem**: Breakpoints work in your launch script but not in code run from the web console.

**Solutions**:

- Make sure the launch script is still running (see the note on keeping the main thread alive above).
- Use `pydevd.settrace()` directly in the code you want to debug, as shown in step 4.

## Related documentation

- [Debug Docker-run Deephaven with PyCharm](./docker-setup.md)
- [Debug pip-installed Deephaven with PyCharm](./pip-setup.md)
- [Debug built-from-source Deephaven with PyCharm](./source-setup.md)
- [Common problems when debugging Deephaven](./common-problems.md)
- [Build and run Deephaven from source code](../../getting-started/launch-build.md)
