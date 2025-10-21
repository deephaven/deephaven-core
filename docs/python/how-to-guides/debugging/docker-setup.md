---
id: docker-setup
title: Debug Docker-run Deephaven with PyCharm
sidebar_label: Docker
---

This guide shows you how to set up a remote debugging server in PyCharm Professional to debug Deephaven code running in a Docker container.

When Deephaven runs in Docker, the engine and user code execute inside a [Docker container](https://www.docker.com/resources/what-container/#:~:text=A%20Docker%20container%20image%20is,tools%2C%20system%20libraries%20and%20settings.). Debugging requires a remote debugging server, like the one available in [PyCharm Professional](https://www.jetbrains.com/pycharm/).

> [!WARNING]
> Debugging Deephaven with PyCharm has only been shown to work for PyCharm major version **2024** or higher. Some issues have been discovered with using PyCharm 2023, and there is no plan to fix these issues.

> [!NOTE]
> PyCharm Professional is a paid product.

## 1. Install `pydevd`

Remote debugging in PyCharm requires the [`pydevd`](https://pypi.org/project/pydevd/) Python package to be installed on the target machine. In this case, the "target machine" is the Docker container where the Deephaven server is running. One way to install `pydevd` is to install it from the Deephaven IDE console:

```python skip-test
import os

os.system("pip install pydevd")
```

This will create a non-persistent installation into the Deephaven container, and those commands will have to be re-run every time the server restarts. For a permanent installation, place these three files in a new, empty directory:

<details>
<summary> requirements.txt </summary>

```text
pydevd
```

</details>

<details>
<summary> Dockerfile </summary>

```text
FROM ghcr.io/deephaven/server:latest
COPY requirements.txt /requirements.txt
RUN pip install -r /requirements.txt && rm /requirements.txt
```

</details>

<details>
<summary> docker-compose.yml </summary>

```text
services:
  deephaven:
    build: .
    ports:
      - '${DEEPHAVEN_PORT:-10000}:10000'
    volumes:
      - ./data:/data
    environment:
      - START_OPTS=-Xmx4g -DAuthHandlers=io.deephaven.auth.AnonymousAuthenticationHandler
```

</details>

When `docker compose up` is run from this directory, it will build a new Deephaven Docker image with `pydevd` permanently installed. We will use this approach in this guide, as it provides a more natural setup for creating a PyCharm project.

## 2. Create PyCharm project

PyCharm requires that its debugger be used from within the context of a PyCharm project. The new directory created in step 1 provides a natural starting point for the new project. The new project should look like this:

![img](../../assets/how-to/debugging/doc-1.png)

## 3. Create debugging server

Next, create a remote debugging server from PyCharm.

Go to **Run > Edit Configurations**, which opens up the **Run/Debug Configurations** window. Click on **+** in the top left corner to add a new configuration. This will bring up a list of configuration types to select. Scroll down and select the **Python Debug Server** configuration option:

![img](../../assets/how-to/debugging/doc-2.png)

Give the configuration a reasonable, memorable name, like `docker-debugging-server`.

Next, set the IDE host name. Since the Deephaven server will be running locally, the hostname is `localhost`.

Now, choose a port for the debug server to run on. Note that this is _not the same_ as the port that the _Deephaven_ server runs on, which is port `10000` by default. Any unused port will work for this - we will use port `4444`.

Finally, a non-empty path mapping is required to map the local PyCharm project to the root of the application in the Docker container. Use a path mapping like the following:

- Local path: `path/to/current-pycharm-project`
- Remote path: `/app`

![img](../../assets/how-to/debugging/doc-3.png)

Click **Apply** and **Ok**, and the new debug server will be immediately available.

## 4. Attach debugger to Deephaven

:::warning
The Deephaven server must be run with anonymous authentication for this kind of debugging to work. Otherwise, the debugger will fail to connect with the server.
:::

Start the debugger in PyCharm by clicking the small green bug icon in the window's top right corner. This will start the debugger and open a debugging console:

![img](../../assets/how-to/debugging/doc-4.png)

The output above states that the debugger is "waiting for process connection". This means that the debugger is not yet attached to the Deephaven process.

If the Deephaven server with `pydevd` installed is already running, navigate to the Deephaven IDE. Otherwise, start the server by opening a new PyCharm terminal and running `docker compose up` in the project root directory:

![img](../../assets/how-to/debugging/doc-5.png)

Attach the debugger to Deephaven by executing the following commands in the Deephaven _console_:

```python skip-test
import pydevd

pydevd.settrace(
    "host.docker.internal",  # Host where PyCharm is running (Docker-specific hostname)
    port=4444,  # Port matching your PyCharm debug config
    suspend=False,  # Don't pause execution immediately
    trace_only_current_thread=False,  # Debug all threads
    stdoutToServer=True,  # Send stdout to PyCharm console
    stderrToServer=True,  # Send stderr to PyCharm console
)
```

![img](../../assets/how-to/debugging/doc-6.png)

Navigate back to PyCharm and verify that Deephaven is connected by finding this new line of output in the debugging console:

```text
Connected to pydev debugger (build 242.23726.102)
```

This means that the debugger is ready to go!

![img](../../assets/how-to/debugging/doc-7.png)

## 5. Use the PyCharm debugger

To interface with the PyCharm debugger from the Deephaven IDE, use the `pydevd.settrace()` function. It behaves like a breakpoint in PyCharm, allowing the program to pause and step through the function at that point:

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

- [Install Deephaven with Docker](../../getting-started/docker-install.md)
- [Common problems with debugging](./common-problems.md)
- [Set up debugger for pip-installed Deephaven](./pip-setup.md)
- [Set up debugger for built-from-source Deephaven](./source-setup.md)
