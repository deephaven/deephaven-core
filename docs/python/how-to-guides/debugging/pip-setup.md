---
id: pip-setup
title: Debug pip-installed Deephaven with PyCharm
sidebar_label: Pip
---

This guide shows you how to set up a debugger for [pip-installed Deephaven](../../getting-started/pip-install.md).

With pip-installed Deephaven, it's common to start the server from a Python script and write Deephaven code in that same script. This setup works well with popular IDEs like [PyCharm](https://www.jetbrains.com/pycharm/) or [VS Code](https://code.visualstudio.com), where you can use the IDE's built-in debugger. Alternatively, you can use the Deephaven IDE itself with pip-installed Deephaven, which requires a remote debugger like the one in [PyCharm Professional](https://www.jetbrains.com/pycharm/buy/?section=commercial&billing=yearly). This guide covers both approaches.

## Setup

Every case covered in this guide assumes that Deephaven is installed in a [Python virtual environment](https://docs.python.org/3/library/venv.html). This is the strongly-recommended approach to using pip-installed Deephaven. Create and activate a virtual environment as follows:

```bash
python3 -m venv dh-venv
source dh-venv/bin/activate
```

This creates a virtual environment called `dh-venv` and makes it usable. The rest of this guide assumes that your virtual environment is called `dh-venv`.

Then, install Deephaven into this environment using `pip`:

```bash
pip install deephaven-server
```

## PyCharm

> [!WARNING]
> Debugging Deephaven with PyCharm has only been shown to work for PyCharm major version **2024** or higher. Some issues have been discovered with using PyCharm 2023, and there is no plan to fix these issues.

With the proper setup, PyCharm's built-in debugger works well with Deephaven scripts hosted in PyCharm. The steps given here will work with both PyCharm Community Edition and PyCharm Professional. If there is an existing PyCharm project set up for using pip-installed Deephaven, skip to step 2.

### 1. Create and configure PyCharm project

First, create a new PyCharm project from the directory containing the `dh-venv` virtual environment. The fresh PyCharm project looks like this:

![img](../../assets/how-to/debugging/pc-1.png)

Next, click on **No interpreter** in the bottom-right-hand corner, click **Add New Interpreter** > **Add Local Interpreter**, select **Existing**, and navigate to the `python` executable in `dh-env`:

![img](../../assets/how-to/debugging/pc-2.png)

Click **OK**.

Verify that the interpreter is configured correctly by opening the terminal and running `which deephaven`. It should return the path of the `deephaven` package inside of `dh-venv`:

![img](../../assets/how-to/debugging/pc-3.png)

### 2. Test debugger

To ensure that PyCharm's debugger will run correctly with Deephaven, create a test script called `script.py` that starts and uses the Deephaven server:

![img](../../assets/how-to/debugging/pc-4.png)

<details>
<summary> Expand for test script </summary>

```python skip-test
from deephaven_server import Server

s = Server(
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

</details>

Then, right-click anywhere in the editor window and select **Debug 'script'**:

![img](../../assets/how-to/debugging/pc-5.png)

This will start the debugger and execute the test script in debug mode. If the test is successful, the debug console will contain output like this:

![img](../../assets/how-to/debugging/pc-6.png)

If the test fails, the Python interpreter may not be configured correctly, or the Deephaven Python package may not be available in the current environment. Follow the previous steps closely to ensure a correctly-configured environment.

### 3. Use debugger with Deephaven

Once the debugger is set up and working, all of the features in PyCharm's debugger will be available. Set breakpoints, step through functions, and inspect source code:

![img](../../assets/how-to/debugging/pc-7.png)

**There are some Deephaven-specific things that must be taken into consideration.** See the guide on [common problems when debugging](./common-problems.md) for information on some subtle problems that can creep into debugging sessions.

## VS Code

:::warning
This has only been shown to work on VS Code 1.94 and higher. Support for debugging Deephaven in older versions of VS Code is not guaranteed.
:::

VS Code's built-in debugger is useful for debugging Deephaven scripts hosted in VS Code's environment. If there is an existing VS Code project set up to work with the `dh-venv` virtual environment, skip to step 2.

### 1. Create and configure VS Code project

First, create a new VS Code project by opening the parent directory of `dh-venv` in the VS Code launch window. This creates a brand new VS Code project:

![img](../../assets/how-to/debugging/vs-1.png)

Then, configure the interpreter by typing **> Python: Select Interpreter** into the search bar at the top:

![img](../../assets/how-to/debugging/vs-2.png)

Select the `dh-venv` environment that was created earlier. VS Code may recommend that environment by default since it's present in the project directory:

![img](../../assets/how-to/debugging/vs-3.png)

Verify that the interpreter is configured correctly by opening a terminal and running `which deephaven`. It should point to the VS Code project directory:

![img](../../assets/how-to/debugging/vs-4.png)

### 2. Test debugger

To ensure that the debugger is configured correctly, create and run a script to start the Deephaven server and run a command. This example is called `script.py`:

![img](../../assets/how-to/debugging/vs-5.png)

<details>
<summary> Expand for test script </summary>

```python skip-test
from deephaven_server import Server

s = Server(
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

</details>

Click on the small downward-facing arrow toward the top-right corner of the screen and select **Python Debugger: Debug Python File**:

![img](../../assets/how-to/debugging/vs-6.png)

This will execute the test script in the debugger for this environment. If the test is successful, it will start the Deephaven server, execute the script, and exit the debugger:

![img](../../assets/how-to/debugging/vs-7.png)

In this case, the debugger is ready for use. If the test fails, ensure that the environment is configured correctly by carefully following the setup in step 1.

### 3. Use debugger with Deephaven

Once the debugger is set up and working, all of the features in VS Code's debugger will be available. Set breakpoints, step through functions, and inspect source code:

![img](../../assets/how-to/debugging/vs-8.png)

**There are some Deephaven-specific things that must be taken into consideration.** See the guide on [common problems when debugging](./common-problems.md) for information on some subtle problems that can creep into debugging sessions.

## Deephaven IDE (more complex)

The Deephaven IDE has no built-in debugger. However, debugging is still possible using a remote debugging server, like the one available in [PyCharm Professional](https://www.jetbrains.com/pycharm/download/?section=mac).

:::note
PyCharm Professional is a paid product.
:::

### 1. Install `pydevd`

First, install `pydevd` into the `dh-venv` virtual environment:

```bash
pip install pydevd
```

Verify that this package is installed and available by starting a Python console and importing the package:

```python skip-test
python3
>>> import pydevd
```

This will display some output like this:

```text
0.00s - Debugger warning: It seems that frozen modules are being used, which may
0.00s - make the debugger miss breakpoints. Please pass -Xfrozen_modules=off
0.00s - to python to disable frozen modules.
0.00s - Note: Debugging will proceed. Set PYDEVD_DISABLE_FILE_VALIDATION=1 to disable this validation.
```

### 2. Create and configure PyCharm project

First, create a new PyCharm project from the directory containing the `dh-venv` virtual environment. The fresh PyCharm project looks like this:

![img](../../assets/how-to/debugging/dh-1.png)

Next, click on **No interpreter** in the bottom-right-hand corner, click **Add New Interpreter** > **Add Local Interpreter**, select **Existing**, and navigate to the `python` executable in `dh-env`:

![img](../../assets/how-to/debugging/dh-2.png)

Click **OK**.

Verify that the interpreter is configured correctly by opening the terminal and running `which deephaven`. It should return the path of the `deephaven` package inside of `dh-venv`:

![img](../../assets/how-to/debugging/dh-3.png)

### 3. Create debugging server

Next, create a remote debugging server from PyCharm.

Go to **Run > Edit Configurations**, which opens up the **Run/Debug Configurations** window. Click on **+** in the top left corner to add a new configuration. This will bring up a list of configuration types to select. Scroll down and select the **Python Debug Server** configuration option:

![img](../../assets/how-to/debugging/dh-4.png)

Give the configuration a reasonable, memorable name. The virtual environment is named `dh-venv`, so the configuration will be named `dh-venv-debugging-server`.

Next, set the IDE host name. Since the pip-installed Deephaven server is going to be running locally, the hostname is `localhost`.

Finally, choose a port for the debug server to run on. Note that this is _not the same_ as the port that the _Deephaven_ server runs on, which is port `10000` by default. Any unused port will work for this - we will use port `4444`.

![img](../../assets/how-to/debugging/dh-5.png)

Click **Apply** and **Ok**, and the new debug server will be immediately available for use.

### 4. Attach debugger to Deephaven

:::warning
The Deephaven server must be run with anonymous authentication for this kind of debugging to work. Otherwise, the debugger will fail to connect with the server.
:::

Start the debugger in PyCharm by clicking the small green bug icon in the window's top right corner. This will start the debugger and open a debugging console:

![img](../../assets/how-to/debugging/dh-6.png)

The output above states that the debugger is "waiting for process connection". This means that the debugger is not yet attached to the Deephaven process.

Now, _from the same virtual environment that the PyCharm debugger is running in_, start the Deephaven server. The easiest way to do this is to open up a new terminal in the current PyCharm project and run `deephaven server`:

![img](../../assets/how-to/debugging/dh-7.png)

To attach the debugger, navigate to the Deephaven IDE and execute the following commands in the _console_:

```python skip-test
import pydevd

pydevd.settrace(
    "localhost",  # Host where PyCharm is running
    port=4444,  # Port matching your PyCharm debug config
    suspend=False,  # Don't pause execution immediately
)
```

![img](../../assets/how-to/debugging/dh-8.png)

This will produce some output, indicating that Deephaven is connected to the debugger:

![img](../../assets/how-to/debugging/dh-9.png)

Navigate back to PyCharm and verify that Deephaven is connected by finding this new line of output in the debugging console:

```text
Connected to pydev debugger (build 242.23726.102)
```

The debugger is ready to be used!

![img](../../assets/how-to/debugging/dh-10.png)

### 5. Use the PyCharm debugger

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

- [Install Deephaven with pip](../../getting-started/pip-install.md)
- [Common problems with debugging](./common-problems.md)
- [Set up debugger for Docker-run Deephaven](./docker-setup.md)
- [Set up debugger for built-from-source Deephaven](./source-setup.md)
