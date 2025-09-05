---
title: How to attach a PyCharm Debugger to Deephaven
sidebar_label: Debugging with PyCharm
---

This is a guide for attaching the PyCharm Python debugger to a running Deephaven server. First, it covers the case where the server was started from the [one-line Docker command](../../getting-started/quickstart.md#1-install-and-launch-deephaven) in the [Quickstart](../../getting-started/quickstart.md). Then, it details attaching the debugger to servers [built from source code](../../getting-started/launch-build.md), using either Docker or Gradle.

> [!NOTE]
> Debugging Deephaven with PyCharm requires using a remote debugging server from PyCharm. This functionality is currently only available in PyCharm Professional, which is a paid product.

## Simple Docker one-liner

The Docker one-liner given in the [Quickstart](../../getting-started/quickstart.md):

```bash
docker run --rm --name deephaven -p 10000:10000 --env START_OPTS=-Dauthentication.psk=YOUR_PASSWORD_HERE ghcr.io/deephaven/server:latest
```

is the easiest way to get started with Deephaven. The resulting Deephaven server runs inside of a Docker container, which can make setting up a debugger a little more complex than it would be for an instance of the [Deephaven production application](../../getting-started/production-application.md). However, PyCharm makes setting up a debugger for a Deephaven instance running in a Docker container as easy as possible.

### 1. Install pydevd in the Deephaven instance

Remote debugging in PyCharm requires the [`pydevd`](https://pypi.org/project/pydevd/) Python package to be installed on the target machine. In this case, the "target machine" is the Docker container where the Deephaven server is running. Installing persistent Python packages in Docker containers can be tricky, so this guide will use a simple, non-persistent installation of `pydevd`. Navigate to the Deephaven IDE and run the following code in the console:

```python skip-test
import os

os.system("pip install pydevd")
```

<LoopedVideo src='../../assets/how-to/debugging/pycharm-debugging/vid1.mp4' />

For a persistent installation of `pydevd` that will exist after the server exits, see the [how-to guide on installing Python packages](../../how-to-guides/install-and-use-python-packages.md).

### 2. Create a PyCharm project

PyCharm requires that its debugger be used from within the context of a PyCharm project. In this case, there is no obvious starting point for a PyCharm project, as there is no Deephaven source code on the host machine since it was run from the Docker one-liner. So, create an empty directory on the host machine and create a new PyCharm project from this directory. PyCharm will automatically create a `main.py` file, which can be discarded - the PyCharm project is only needed for its debugger.

<LoopedVideo src='../../assets/how-to/debugging/pycharm-debugging/vid2.mp4' />

### 3. Set up the debugger

Next, create a remote debugging server from PyCharm.

- Go to **Run > Edit Configurations**, which will open up the **Run/Debug Configurations** window.
- In the window's top-left corner, click on **+** to add a new configuration. This will bring up a list of configuration types to select.
- Scroll down and select the **Python Debug Server** configuration option. This will bring up a new window with customization options for the new configuration.

Give the configuration a reasonable, memorable name, like `docker-dh-debugging-server`.

Next, set the IDE host name. Since the Deephaven server will be running locally, the hostname is `localhost`.

Now, choose a port for the debug server to run on. Note that this is _not the same_ as the port that the _Deephaven_ server runs on, which is port `10000` by default. Any unused port will work for this - we will use port `4444`.

Finally, a non-empty path mapping is required to map the local PyCharm project to the root of the application in the Docker container. Use a path mapping like the following:

- Local path: `path/to/empty-pycharm-directory`
- Remote path: `/app`

Click **Apply** and **Ok**, and the new debug server will be immediately available.

<LoopedVideo src='../../assets/how-to/debugging/pycharm-debugging/vid3.mp4' />

### 4. Attach the debugger to Deephaven

To start the debugger in PyCharm, click the small green bug icon in the window's top right corner. This will start the debugger and open a debugging console. The console will contain output that looks something like this:

```text
import sys; print('Python %s on %s' % (sys.version, sys.platform))
Starting debug server at port 4,444
Waiting for process connection…
Use the following code to connect to the debugger:
import pydevd_pycharm
pydevd_pycharm.settrace('localhost', port=4444, stdoutToServer=True, stderrToServer=True)
```

The output above states that the debugger is "waiting for process connection". This means that the debugger is not yet attached to the Deephaven process. To attach the debugger, navigate to the Deephaven IDE and execute the following commands in the _console_:

```python skip-test
import pydevd

pydevd.settrace(
    "host.docker.internal",
    port=4444,
    suspend=False,
    trace_only_current_thread=False,
    stdoutToServer=True,
    stderrToServer=True,
)
```

Running these commands in the Deephaven IDE produces a new line of output in the PyCharm debugging console:

```text
Connected to pydev debugger (build 232.9921.89)
```

This means that the debugger is ready to go!

<LoopedVideo src='../../assets/how-to/debugging/pycharm-debugging/vid4.mp4' />

## Built-from-source Deephaven

The instructions for [building Deephaven from source](../../getting-started/launch-build.md) walk through cloning the [deephaven-core](https://github.com/deephaven/deephaven-core) repository, building the server with [Gradle](https://gradle.org), and running the system with a simple Gradle command. There is also a simple way to build Deephaven from source using [Docker](https://www.docker.com), though this is less frequently used. In either case, a local clone of the [Deephaven Core source code](https://github.com/deephaven/deephaven-core) is required. The remainder of this guide assumes a source code clone in a local `deephaven-core` directory.

### Run with Docker

If the command being used to run the Deephaven server is `docker compose up`, called from the root directory of the source code, then the Deephaven server is being run with Docker inside a Docker container. Attaching a debugger to the Deephaven server in this case is almost identical to the Docker one-liner case presented above, because both cases have the Deephaven server running inside a Docker container. There are two key differences:

1. There is now a source code clone in a local `deephaven-core` directory, so the PyCharm project should be created from this directory, rather than creating an empty directory as above. So, step 2 given above should be modified to create a PyCharm project from `deephaven-core`.

2. Similarly, the path mapping given in step 3 should be modified in accordance with the project root being changed to `deephaven-core`. In particular, the path mappings in this case should be

- Local path: `path/to/deephaven-core`
- Remote path: `/app`

Outside of these two small differences, the process for attaching the PyCharm debugger to Deephaven is exactly the same, whether Deephaven is run from the Docker one-liner or built from source and run with Docker.

### Run with Gradle

Attaching the PyCharm debugger to Deephaven run with Gradle is distinct from the Docker cases because the server ends up running on the local machine rather than in a Docker container. This simplifies setting up the debugger, but adds the additional complexity of requiring (or _strongly_ recommending) the use of a virtual Python environment for installing `pydevd` and running Deephaven. Here's a step-by-step guide through the process.

#### 1. Set up the Python environment

First, create a Python virtual environment [venv](https://docs.python.org/3/library/venv.html) in the local `deephaven-core` directory. This environment will be used to hold a `pydevd` installation for local debugging. The package is installed with `pip`:

```bash
pip install pydevd
```

Verify that this package is installed and available by starting a Python console and importing the package:

```python skip-test
import pydevd
```

This will display some output like this:

```text
0.00s - Debugger warning: It seems that frozen modules are being used, which may
0.00s - make the debugger miss breakpoints. Please pass -Xfrozen_modules=off
0.00s - to python to disable frozen modules.
0.00s - Note: Debugging will proceed. Set PYDEVD_DISABLE_FILE_VALIDATION=1 to disable this validation.
```

<LoopedVideo src='../../assets/how-to/debugging/pycharm-debugging/vid5.mp4' />

#### 2. Create a PyCharm project

To debug Deephaven from PyCharm, create a PyCharm project from the source code clone that the server will be run from. This PyCharm project must be based on the _same_ code used to build and run the server, as otherwise, the debugger may be seeing a different version of the source code than the server is using, which would not be useful for pinpointing bugs. If there is an existing PyCharm project built from `deephaven-core`, great! If not, create a new project by following the instructions [here](https://www.jetbrains.com/help/pycharm/setting-up-your-project.html). In either case, it's critical to set the project to use Python environment discussed above, so that the project is using the same Python environment as the Deephaven server.

<LoopedVideo src='../../assets/how-to/debugging/pycharm-debugging/vid6.mp4' />

#### 3. Set up the debugger

Now, we need to create a remote debugging server from PyCharm.

- Go to **Run > Edit Configurations**, which will open up the **Run/Debug Configurations** window.
- In the window's top-left corner, click on **+** to add a new configuration. This will bring up a list of configuration types to select.
- Scroll down and select the **Python Debug Server** configuration option. This will bring up a new window with customization options for the new configuration.

First, give the configuration a reasonable, memorable name. The virtual environment is named `my-debugging-venv`, so we will name the debug server to be used with that environment `my-debugging-server`.

Next, set the IDE host name. Since the Deephaven server is going to be running locally, the hostname is `localhost`.

Finally, choose a port for the debug server to run on. Note that this is _not the same_ as the port that the _Deephaven_ server runs on, which is port `10000` by default. Any unused port will work for this - we will use port `4444`.

Click **Apply** and **Ok**, and your new debug server will be immediately available for use.

<LoopedVideo src='../../assets/how-to/debugging/pycharm-debugging/vid7.mp4' />

#### 4. Attach the debugger to Deephaven

To start the debugger in PyCharm, click the small green bug icon in the window's top right corner. This will start the debugger and open a debugging console. The console will contain output that looks something like this:

```text
import sys; print('Python %s on %s' % (sys.version, sys.platform))
Starting debug server at port 4,444
Waiting for process connection…
Use the following code to connect to the debugger:
import pydevd_pycharm
pydevd_pycharm.settrace('localhost', port=4444, stdoutToServer=True, stderrToServer=True)
```

The output above states that the debugger is "waiting for process connection". This means that the debugger is not yet attached to the Deephaven process. To attach the debugger, navigate to the Deephaven IDE and execute the following commands in the _console_:

```python skip-test
import pydevd

pydevd.settrace("localhost", port=4444, suspend=False)
```

These commands a new line of output in the PyCharm debugging console:

```text
Connected to pydev debugger (build 232.9921.89)
```

The debugger is ready to be used!

<LoopedVideo src='../../assets/how-to/debugging/pycharm-debugging/vid8.mp4' />

## Use the PyCharm debugger

To interface with the PyCharm debugger from the Deephaven IDE, we use the `pydevd.settrace()` function. In the code sample below, we call this function before a faulty line of code, and it will enable us to step through the stack trace of that function call in PyCharm. Run the following code in the Deephaven IDE:

```python skip-test
from deephaven import empty_table

t = empty_table(10).update("X = ii")
pydevd.settrace()
t2 = t.update("Y = Math.sin(x)")
```

Navigating over to PyCharm, the debugger's full suite of capabilities is on offer. It can assist in stepping through Deephaven source code, setting breakpoints to halt execution, inspecting variables and intermediate values from deep within function calls, and much more. Many resources are available online for using PyCharm's debugger, and [this Jetbrains guide](https://www.jetbrains.com/help/pycharm/using-debug-console.html) is a good starting point.

## Related documentation

- [How to triage errors](../triage-errors.md)
- [How to decipher Python error messages](../python-errors.md)
