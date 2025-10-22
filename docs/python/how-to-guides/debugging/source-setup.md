---
id: source-setup
title: Debug built-from-source Deephaven with Pycharm
sidebar_label: Source
---

It is common to build Deephaven from its source code for development tasks. In such cases, debugging both user-level code and Deephaven's source code can be useful. This guide walks through setting up a debugging server for Deephaven using [PyCharm Professional](https://www.jetbrains.com/pycharm/).

> [!WARNING]
> Debugging Deephaven with PyCharm has only been shown to work for PyCharm major version **2024** or higher. Some issues have been discovered with using PyCharm 2023, and there is no plan to fix these issues.

> [!NOTE]
> PyCharm professional is a paid product.

## Setup

This guide assumes that Deephaven is built using the steps in the [guide on building Deephaven from source](../../getting-started/launch-build.md). Following these steps produces a local clone of [Deephaven Core](https://github.com/deephaven/deephaven-core) in a directory called `deephaven-core`, and a Python virtual environment for the Deephaven installation. This guide will assume that virtual environment is called `source-dh-venv`.

## 1. Install `pydevd`

First, install the `pydevd` Python package into the `source-dh-venv` virtual environment using pip:

```bash
pip install pydevd
```

It's critical that this is installed into the same virtual environment as the source build of Deephaven. The debugger will not work properly if this is not the case.

## 2. Create PyCharm project

To debug Deephaven from PyCharm, create a PyCharm project from the source code clone that the server will be run from. This PyCharm project must be based on the _same_ code used to build and run the server.

From PyCharm's launch window, click **Open** and navigate to the `deephaven-core` directory. The new project should look like this:

![img](../../assets/how-to/debugging/source-1.png)

PyCharm should automatically set the project interpreter to be the virtual environment created in step 1. Confirm this by going to **PyCharm** > **Settings** > **Project: deephaven-core** > **Python Interpreter**. Verify that the path selected matches the path of `source-dh-venv`:

![img](../../assets/how-to/debugging/source-2.png)

## 3. Configure debugger

Next, create a remote debugging server from PyCharm.

Go to **Run > Edit Configurations**, which opens up the **Run/Debug Configurations** window. Click on **+** in the top left corner to add a new configuration. This will bring up a list of configuration types to select. Scroll down and select the **Python Debug Server** configuration option:

![img](../../assets/how-to/debugging/source-3.png)

Give the configuration a reasonable, memorable name. The virtual environment is named `source-dh-venv`, so the configuration will be named `source-dh-venv-debugging-server`.

Next, set the IDE host name. Since the Deephaven server is going to be running locally, the hostname is `localhost`.

Finally, choose a port for the debug server to run on. Note that this is _not the same_ as the port that the _Deephaven_ server runs on, which is port `10000` by default. Any unused port will work for this - we will use port `4444`.

![img](../../assets/how-to/debugging/source-4.png)

Click **Apply** and **Ok**, and the new debug server will be immediately available for use.

## 4. Attach debugger to Deephaven

:::warning
The Deephaven server must be run with anonymous authentication for this kind of debugging to work. Otherwise, the debugger will fail to connect with the server.
:::

Start the debugger in PyCharm by clicking the small green bug icon in the window's top right corner. This will start the debugger and open a debugging console:

![img](../../assets/how-to/debugging/source-5.png)

The output above states that the debugger is "waiting for process connection". This means that the debugger is not yet attached to the Deephaven process.

Now, _from the same virtual environment that the PyCharm debugger is running in_, start the Deephaven server. The easiest way to do this is to open up a new terminal in the current PyCharm project and run `./gradlew server-jetty-app:run -Panonymous`:

![img](../../assets/how-to/debugging/source-6.png)

To attach the debugger, navigate to the Deephaven IDE and execute the following commands in the _console_:

```python skip-test
import pydevd

pydevd.settrace(
    "localhost",  # Host where PyCharm is running
    port=4444,  # Port matching your PyCharm debug config
    suspend=False,  # Don't pause execution immediately
)
```

![img](../../assets/how-to/debugging/source-7.png)

This will produce some output, indicating that Deephaven is connected to the debugger:

![img](../../assets/how-to/debugging/source-8.png)

Navigate back to PyCharm and verify that Deephaven is connected by finding this new line of output in the debugging console:

```text
Connected to pydev debugger (build 242.23726.102)
```

The debugger is ready to be used!

![img](../../assets/how-to/debugging/source-9.png)

## 5. Use the PyCharm debugger

To interface with the PyCharm debugger from the Deephaven IDE, use the `pydevd.settrace()` function. This function is used as a breakpoint, enabling full-fledged debugging on scripts hosted in the Deephaven IDE:

```python skip-test
from deephaven import empty_table


def udf(x) -> int:
    # acts like a breakpoint
    pydevd.settrace()
    y = x + 1
    return y


t = empty_table(10).update("X = ii")

t_new = t.update("Y = udf(X)")
```

Navigating over to PyCharm, the debugger's full suite of capabilities is on offer. It can assist in stepping through Deephaven source code, setting breakpoints to halt execution, inspecting variables and intermediate values from deep within function calls, and much more. Many resources are available online for using PyCharm's debugger, and [this Jetbrains guide](https://www.jetbrains.com/help/pycharm/using-debug-console.html) is a good starting point.

**There are some Deephaven-specific things to consider when debugging**. Check out [this guide](./common-problems.md) for some common problems when debugging Deephaven.

## Related documentation

- [Build Deephaven from source code](../../getting-started/launch-build.md)
- [Common problems with debugging](./common-problems.md)
- [Set up debugger for Docker-run Deephaven](./docker-setup.md)
- [Set up debugger for pip-installed Deephaven](./pip-setup.md)
