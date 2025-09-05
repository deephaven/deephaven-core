---
title: Install and run Deephaven with pip
sidebar_label: pip
---

[pip](https://pypi.org/project/pip/) is the most popular package manager for Python. Like with other Python packages, you can use it to install Deephaven.

> [!NOTE]
> pip-installed Deephaven is not recommended for production applications. For other installation options more suited for production, see:
>
> - [Docker quickstart](./docker-install.md): Run Deephaven from Docker
> - [Build from source](./launch-build.md): Build Deephaven from source code
> - [Production application](./production-application.md): Run the production application (build artifacts)

## Supported operating systems

Deephaven is only supported on:

- Linux
- MacOS
- Windows 10 or 11 (requires [WSL 2 (Windows Subsystem for Linux v2)](https://learn.microsoft.com/en-us/windows/wsl/install))

## Prerequisites

pip-installed Deephaven requires the following package versions:

| Package | Recommended Version | Required Version |
| ------- | ------------------- | ---------------- |
| java    | latest LTS          | >= 17            |
| python  | latest LTS          | >= 3.9           |

Once the prerequisite packages have been installed, you must also set your `JAVA_HOME` environment variable appropriately.

Once you've completed these steps, you can install Deephaven with pip:

```bash
pip3 install --upgrade setuptools wheel
pip3 install deephaven-server
```

> [!NOTE]
> Deephaven recommends the use of [virtual environments](https://docs.python.org/3/library/venv.html) when installing Python packages.

## Start a Deephaven server

There are two ways to start a Deephaven server after installing it with pip. You can start it from Python (either a script or an interactive session) or from the command line using the Deephaven CLI. The Deephaven CLI is just a wrapper around the Python API, so both methods are equivalent in functionality.

### From Python

Start Deephaven from Python with the following code:

```python skip-test
from deephaven_server import Server

# Start a server with 4GB RAM on port 10000 and the default PSK authentication
s = Server(port=10000, jvm_args=["-Xmx4g"]).start()
```

If no pre-shared key is specified, the server generates a random one when started. You can instead specify the key as another JVM argument:

```python skip-test
from deephaven_server import Server

# Start a server with 4GB RAM on port 10000 and `PythonR0cks!` as the pre-shared key
s = Server(port=10000, jvm_args=["-Xmx4g", "-Dauthentication.psk=PythonR0cks!"]).start()
```

You can also enable [anonymous authentication](../how-to-guides/authentication/auth-anon.md), which allows you to connect to the server without a pre-shared key:

```python skip-test
from deephaven_server import Server

# Start a server with 4GB RAM on port 10000 and anonymous authentication
s = Server(
    port=10000,
    jvm_args=[
        "-Xmx4g",
        "-DAuthHandlers=io.deephaven.auth.AnonymousAuthenticationHandler",
    ],
).start()
```

> [!NOTE]
> Anonymous authentication provides no application security.

### From the command line

When Deephaven is installed with pip, it comes with a command line interface (CLI). The CLI allows you to start Deephaven servers without instantiating a Python script or using the Python interpreter.

The following command shows a basic usage of the Deephaven CLI:

```bash
# Start a server with 4GB RAM on port 10000 and the default PSK authentication
deephaven server --port 10000 --jvm-args "-Xmx4g"
```

There are a number of available options:

- `--help`: Show a help message and exit.
- `server`: Start a Deephaven server. Additional options include:
  - `--host TEXT`: The host on which to start the server. Default is `localhost`.
  - `--port INTEGER`: The port on which to start the server. Default is 10000.
  - `--jvm-args TEXT`: Additional JVM arguments to pass to the server. For example, `-Xmx4g` to allocate 4GB of memory,
  - `--browser` / `--no-browser`: Whether or not to open a browser automatically. Default is `--browser`, which automatically opens your default browser when launching Deephaven.
  - `--extra-classpath TEXT`: Additional classpath entries to add to the server's classpath.
  - `--help`: Show a help message about the `server` command and exit.

For example, the following command sets the pre-shared key to `PythonR0cks!`:

```bash
# Start a server with 4GB RAM on port 10000 and `PythonR0cks!` as the pre-shared key
deephaven server --port 10000 --jvm-args "-Xmx4g -Dauthentication.psk=PythonR0cks!"
```

The following command starts a server with anonymous authentication:

```bash
# Start a server with 4GB RAM on port 10000 and anonymous authentication
deephaven server --port 10000 --jvm-args "-Xmx4g -DAuthHandlers=io.deephaven.auth.AnonymousAuthenticationHandler"
```

## M2 Macs

If you're trying to use pip-installed Deephaven on an M2 MacBook, you must add the argument `"-Dprocess.info.system-info.enabled=false"` to the `jvm_args` list, as in:

```python skip-test
s = Server(port=10000, jvm_args=["-Xmx4g", "-Dprocess.info.system-info.enabled=false"])
```

```bash
deephaven server --port 10000 --jvm-args "-Xmx4g -Dprocess.info.system-info.enabled=false"
```

## Example scripts

This section contains a couple of scripts that demonstrate how to use Deephaven with pip. The scripts assume that you have already started a Deephaven server as shown above, as you _must_ start the server before performing any Deephaven operations - including imports.

This script creates three streaming tables. The first table (`t`) steadily increases in size, the second table (`t_last`) contains the most recent timestamp for each label, and the third table (`t_join`) joins the most recent timestamp onto the first table.

```python ticking-table order=null
from deephaven import time_table

t = time_table("PT1S").update("A = i%2==0 ? `A` : `B`")
t_last = t.last_by("A")
t_join = t.natural_join(t_last, on="A", joins=["LastTime=Timestamp"])

print(t_join)
```

When running Python scripts from the command line, it's recommended to do so in [interactive mode](https://docs.python.org/3/tutorial/interpreter.html#interactive-mode) so the Python session stays up and running. This ensures the Deephaven server does not stop when the script finishes executing:

```bash
# Interactive mode is enabled with the `-i` flag
python3 -i example.py
```

![The server has been started via the command line](../assets/how-to/pip-2.png)
![The Deephaven web UI](../assets/how-to/pip-1.png)

This next script creates a `left` table with employee data and a `right` table with department data. It then joins them on the `DeptID` column:

```python order=left,right,table
from deephaven import new_table
from deephaven.column import string_col, int_col
from deephaven.constants import NULL_INT

left = new_table(
    [
        string_col(
            "LastName", ["Rafferty", "Jones", "Steiner", "Robins", "Smith", "Rogers"]
        ),
        int_col("DeptID", [31, 33, 33, 34, 34, NULL_INT]),
        string_col(
            "Telephone",
            [
                "(347) 555-0123",
                "(917) 555-0198",
                "(212) 555-0167",
                "(952) 555-0110",
                None,
                None,
            ],
        ),
    ]
)

right = new_table(
    [
        int_col("DeptID", [31, 33, 34, 35]),
        string_col("DeptName", ["Sales", "Engineering", "Clerical", "Marketing"]),
        string_col(
            "Telephone",
            ["(646) 555-0134", "(646) 555-0178", "(646) 555-0159", "(212) 555-0111"],
        ),
    ]
)

table = left.join(
    table=right, on=["DeptID"], joins=["DeptName,DeptTelephone=Telephone"]
)
```

## What to do next?

import { TutorialCTA } from '@theme/deephaven/CTA';

<div className="row">
<TutorialCTA to="/core/docs/getting-started/crash-course/overview" />
</div>

## Related documentation

- [Build and launch Deephaven from source code](./launch-build.md)
- [Create a new table](../how-to-guides/new-and-empty-table.md#new_table)
- [Joins: Exact and Relational](../how-to-guides/joins-exact-relational.md)
- [Joins: Time-Series and Range](../how-to-guides/joins-timeseries-range.md)
- [How to install Python packages](../how-to-guides/install-and-use-python-packages.md)
