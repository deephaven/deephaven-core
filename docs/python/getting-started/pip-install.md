---
title: Install and run Deephaven with pip
---

[pip](https://pypi.org/project/pip/) is the most popular package manager for Python. This guide shows how to install Deephaven with pip, start a Deephaven server from Python or the command line, and run example queries in it.

> [!NOTE]
> Deephaven doesn't recommend pip-installed Deephaven for production applications. For installation options better suited to production, see:
>
> - [Docker quickstart](./docker-install.md): Run Deephaven from Docker
> - [Build from source](./launch-build.md): Build Deephaven from source code
> - [Production application](./production-application.md): Run Deephaven from prebuilt release artifacts

## Supported operating systems

Deephaven supports only the following operating systems:

- Linux
- macOS
- Windows 10 or 11 (requires [WSL 2 (Windows Subsystem for Linux v2)](https://learn.microsoft.com/en-us/windows/wsl/install))

> [!WARNING]
> WSL 2's default time-sync setup can cause spurious 10–20-second clock jumps. These jumps stall Deephaven's ticking tables, which are tables that update live as new data arrives. Before running Deephaven on WSL 2, apply one of the [time-sync workarounds](../reference/community-questions/wsl2-clock-drift.md).

## Prerequisites

pip-installed Deephaven requires:

| Software | Recommended version | Required version |
| -------- | ------------------- | ---------------- |
| Java     | latest LTS          | >= 17            |
| Python   | 3.13                | >= 3.9           |

After installing Java and Python, set the `JAVA_HOME` environment variable to your Java installation.

## Install Deephaven

Deephaven recommends installing Python packages in a [virtual environment](https://docs.python.org/3/library/venv.html). To create and activate one:

```bash
python3 -m venv .venv
source .venv/bin/activate
```

Upgrade `setuptools` and `wheel`, then install the `deephaven-server` package:

```bash
pip3 install --upgrade setuptools wheel
pip3 install deephaven-server
```

## Start a Deephaven server

There are two ways to start a Deephaven server after installing it with pip. You can start it from Python (either a script or an interactive session) or from the command line using the Deephaven command line interface (CLI). The Deephaven CLI is a wrapper around the Python API.

By default, the server uses [pre-shared key (PSK) authentication](../how-to-guides/authentication/auth-psk.md), so the web IDE and clients need a password-like key to connect. If you don't set a key, the server generates a random one at startup.

> [!NOTE]
> On M2 Macs, add `-Dprocess.info.system-info.enabled=false` to the JVM arguments (`jvm_args` in Python, `--jvm-args` on the command line).

### From Python

When you start the server from Python, it doesn't print the generated key to the console, so set your own key with a JVM argument.

The `Server` constructor takes the following parameters: `host`, `port`, `jvm_args`, `default_jvm_args`, and `extra_classpath`. They correspond to the CLI's `--host`, `--port`, `--jvm-args`, `--default-jvm-args`, and `--extra-classpath` options, described in [From the command line](#from-the-command-line). The constructor takes lists where the CLI takes space-separated strings. The constructor has no equivalent of `--browser`.

The following code starts the server and sets the key to `PythonR0cks!`:

```python skip-test
from deephaven_server import Server

# Start a server with a 4 GB maximum heap on port 10000 and `PythonR0cks!` as the pre-shared key
s = Server(port=10000, jvm_args=["-Xmx4g", "-Dauthentication.psk=PythonR0cks!"])
s.start()
```

After the server starts, open the web IDE at [http://localhost:10000/ide/](http://localhost:10000/ide/) and enter the key you set. If you chose a different port, use it in place of `10000`.

You can also enable [anonymous authentication](../how-to-guides/authentication/auth-anon.md), which allows you to connect to the server without a pre-shared key:

```python skip-test
from deephaven_server import Server

# Start a server with a 4 GB maximum heap on port 10000 and anonymous authentication
s = Server(
    port=10000,
    jvm_args=[
        "-Xmx4g",
        "-DAuthHandlers=io.deephaven.auth.AnonymousAuthenticationHandler",
    ],
)
s.start()
```

> [!NOTE]
> Anonymous authentication provides no application security.

The Deephaven server runs only as long as the Python process that started it. When you run a Python script from the command line, use [interactive mode](https://docs.python.org/3/tutorial/interpreter.html#interactive-mode) so the Python session and the Deephaven server keep running after the script finishes. For example, if you save either startup snippet above as `example.py`, run:

```bash
# The `-i` flag enables interactive mode
python3 -i example.py
```

> [!CAUTION]
> In a script that starts the server, create the `Server` object, which starts the JVM, **before** importing the `deephaven` package or performing any Deephaven operations. If you import `deephaven` first, you get a `RuntimeError` saying that the Deephaven server hasn't been initialized.

### From the command line

Installing Deephaven with pip also installs the `deephaven` CLI, which starts a Deephaven server without a Python script.

The following command starts a server with the Deephaven CLI:

```bash
# Start a server with a 4 GB maximum heap on port 10000 and the default PSK authentication
deephaven server --port 10000 --jvm-args "-Xmx4g"
```

The command prints the URL of the web IDE, including the pre-shared key, and by default opens that URL in your browser so you're logged in automatically. With `--no-browser`, open the printed URL yourself. The server keeps running until you press <kbd>Ctrl+C</kbd>.

The `deephaven` CLI accepts the following options and commands:

- `--help`: Show a help message and exit.
- `server`: Start a Deephaven server. Additional options include:
  - `--host TEXT`: The network interface the server binds to. If you don't set it, the server listens on all network interfaces, and the printed URL uses `localhost`.
  - `--port INTEGER`: The port on which to start the server. Default is `10000`.
  - `--jvm-args TEXT`: Additional JVM arguments to pass to the server, separated by spaces. For example, `-Xmx4g` sets the maximum heap size to 4 GB.
  - `--default-jvm-args TEXT`: Advanced JVM arguments to use in place of the defaults that Deephaven recommends (for example, garbage collector settings). Most users don't need this.
  - `--browser` / `--no-browser`: Whether to open your default browser when the server starts. Default is `--browser`.
  - `--extra-classpath TEXT`: Additional classpath entries to add to the server's classpath. Separate multiple entries with spaces, not `:`. Each entry can be a glob pattern, such as `/path/to/libs/*.jar`. Entries that match no file are silently ignored.
  - `--help`: Show a help message about the `server` command and exit.

For example, the following command sets the [pre-shared key](../how-to-guides/authentication/auth-psk.md) to `PythonR0cks!` instead of a random one:

```bash
# Start a server with a 4 GB maximum heap on port 10000 and `PythonR0cks!` as the pre-shared key
deephaven server --port 10000 --jvm-args "-Xmx4g -Dauthentication.psk=PythonR0cks!"
```

The following command starts a server with [anonymous authentication](../how-to-guides/authentication/auth-anon.md), which provides no application security:

```bash
# Start a server with a 4 GB maximum heap on port 10000 and anonymous authentication
deephaven server --port 10000 --jvm-args "-Xmx4g -DAuthHandlers=io.deephaven.auth.AnonymousAuthenticationHandler"
```

## Example scripts

This section contains two scripts to run on your pip-installed Deephaven server. If you started the server with the CLI, paste these scripts into the web IDE. If you started it from Python, run them in that Python session after the server has started. To run a script as a standalone file instead, add it to `example.py` after the startup code from the [From Python](#from-python) section. Put the script's `deephaven` imports after the line that starts the server. Then run the file with `python3 -i example.py`.

This script creates three ticking tables (tables that update live as new data arrives) with [`time_table`](../reference/table-operations/create/timeTable.md), [`last_by`](../reference/table-operations/group-and-aggregate/lastBy.md), and [`natural_join`](../reference/table-operations/join/natural-join.md):

- `t` adds a row every second, so it steadily grows.
- `t_last` holds the most recent row for each value in column `A`.
- `t_join` adds that most recent `Timestamp` to `t` as the `LastTime` column.

```python ticking-table order=null
from deephaven import time_table

t = time_table("PT1S").update("A = i % 2 == 0 ? `A` : `B`")
t_last = t.last_by("A")
t_join = t.natural_join(t_last, on="A", joins=["LastTime = Timestamp"])

print(t_join)
```

If you run the script with `python3 -i`, the terminal shows the server starting and a one-line summary of `t_join`, including its row count and the start of its column list. In every case, the web IDE shows the three ticking tables:

![Terminal output from a Python session that started the Deephaven server and printed a summary of t_join](../assets/how-to/pip-2.png)
![The Deephaven web IDE](../assets/how-to/pip-1.png)

This next script creates a `left` table with employee data and a `right` table with department data. It then joins them on the `DeptID` column with [`join`](../reference/table-operations/join/join.md). Run it the same way as the first script.

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
    table=right, on=["DeptID"], joins=["DeptName", "DeptTelephone = Telephone"]
)
```

## What to do next

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
