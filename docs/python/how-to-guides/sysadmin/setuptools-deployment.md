---
title: Package a Deephaven Python project
---

[Python packaging](https://packaging.python.org/en/latest/) turns a project containing Python code into a distribution that can be installed with `pip`. A distribution can provide an importable library, command-line tools, or both. This guide shows how to package Python projects that use Deephaven. It follows the conventions of the [Python Packaging User Guide](https://packaging.python.org/en/latest/tutorials/packaging-projects/) and uses [setuptools](https://setuptools.pypa.io/en/latest/) as the build backend.

A packaged project can work with a Deephaven server in one of two ways:

- **Embedded server:** [`deephaven-server`](https://pypi.org/project/deephaven-server/) starts a Deephaven server and its JVM inside the Python process. The server-side `deephaven` API is available in that same process once the server has started.
- **Remote client:** [`pydeephaven`](https://pypi.org/project/pydeephaven/) connects a Python process to a Deephaven server that is already running, such as one started with Docker or `pip`-installed `deephaven-server` in another process. The client does not start a server and does not provide the server-side `deephaven` API; it works with tables through a [`Session`](/core/client-api/python/code/pydeephaven.session.html#pydeephaven.session.Session).

The packaging tooling is the same for both. What differs is the dependency you declare and, for the embedded server, the order in which modules are imported. This guide shows a library package and a command-line package for each model.

The code in this guide is condensed from the [deephaven-python-packaging](https://github.com/deephaven-examples/deephaven-python-packaging) repository, which contains complete, runnable versions of each package along with sample data and a README for each. The repository versions add input validation and error handling; the snippets here leave those out to stay focused on packaging. Clone it to try the examples or to use one as a starting point:

```bash
git clone https://github.com/deephaven-examples/deephaven-python-packaging.git
cd deephaven-python-packaging
```

## Python packaging terms

A Python **project** is the source code and configuration you develop. Building it produces a **distribution package**, usually a [wheel](https://packaging.python.org/en/latest/glossary/#term-Wheel) (`.whl`) or [source distribution](https://packaging.python.org/en/latest/glossary/#term-Source-Distribution-or-sdist), which users install with `pip`. An **import package** is the code users import in Python. Its name can differ from the distribution name: for example, a project distributed as `my-dh-cli` can provide the import package `my_dh_cli`. The [Python Packaging Glossary](https://packaging.python.org/en/latest/glossary/) defines these and other packaging terms.

The standard project configuration file is `pyproject.toml`. Its `[project]` table is defined by the [`pyproject.toml` specification](https://packaging.python.org/en/latest/specifications/pyproject-toml/), and the Python Packaging User Guide's [Writing your `pyproject.toml`](https://packaging.python.org/en/latest/guides/writing-pyproject-toml/) walks through each field. Setuptools-specific options, such as package discovery, are documented in the [setuptools `pyproject.toml` configuration guide](https://setuptools.pypa.io/en/latest/userguide/pyproject_config.html). The examples use the [src layout](https://packaging.python.org/en/latest/discussions/src-layout-vs-flat-layout/), which keeps importable code in `src/`:

```
my_project/
├── src/
│   └── my_package/
│       ├── __init__.py
│       └── module.py
├── pyproject.toml
└── README.md
```

## Package code for an embedded server

Use `deephaven-server` when the Python program needs to start a Deephaven server itself. This is the right fit for a self-contained tool, a batch job, or a script that should run without any other infrastructure in place.

One rule shapes every embedded-server package: **`deephaven` cannot be imported until a server has started in the same process.** The `deephaven` module binds to a running JVM at import time, so an import that happens too early fails. In practice this means that a library's modules can import `deephaven` freely, because they are only imported after the calling program has started a server, but a package that defines commands must keep `deephaven` out of module scope in `__init__.py` and in the command modules themselves, and import it inside the function that runs after `server.start()`. The examples below show both cases.

### Embedded-server library

A library package provides functions for another program to import. It does not start a server itself; that is the calling program's job, which keeps the library flexible, because the caller decides how the server is configured, which port it uses, and how much memory the JVM gets. The full example is [`my_dh_library`](https://github.com/deephaven-examples/deephaven-python-packaging/tree/main/my_dh_library) in the example repository.

Declare `deephaven-server` as a dependency. It supplies the `deephaven` module through its `deephaven-core` dependency, so the library does not need to list `deephaven-core` separately. The following configuration uses setuptools and its [automatic package discovery](https://setuptools.pypa.io/en/latest/userguide/package_discovery.html) to find the import package under `src/`. The `[build-system]` and `[tool.setuptools.packages.find]` tables are the same for every example in this guide, so later listings show only the tables that differ:

```toml
[build-system]
requires = ["setuptools>=61"]
build-backend = "setuptools.build_meta"

[project]
name = "my-dh-library"
version = "0.1.0"
description = "Reusable Deephaven query functions"
readme = "README.md"
requires-python = ">=3.9"
dependencies = [
  "deephaven-server",
]

[tool.setuptools.packages.find]
where = ["src"]
```

Set the Python version and dependency constraints to versions you test and support. A lower bound such as `deephaven-server>=42` declares compatibility; it does not lock the environment to a repeatable set of versions. If you need that, use a lock file or a pinned requirements file alongside the package. The syntax for these constraints is defined by the [dependency specifiers](https://packaging.python.org/en/latest/specifications/dependency-specifiers/) and [version specifiers](https://packaging.python.org/en/latest/specifications/version-specifiers/) specifications.

Because a library is only imported after the caller has started a server, its modules can import `deephaven` at the top level. For example, `src/my_dh_library/queries.py` might contain:

```python skip-test
from deephaven.table import Table


def filter_by_threshold(table: Table, column: str, threshold: float) -> Table:
    """Keep rows where column > threshold."""
    return table.where([f"{column} > {threshold}"])
```

The calling program starts the server, then imports the library:

```python skip-test
from deephaven_server import Server

server = Server(port=10000)
server.start()

from deephaven import read_csv
from my_dh_library.queries import filter_by_threshold

scores = read_csv("data/sample.csv")
high_scores = filter_by_threshold(scores, "Score", 75.0)
```

### Embedded-server command

A command-line program that uses the embedded server is a complete, standalone application: when a user runs it, it starts a server, does its work, and exits. The full example is [`my_dh_cli`](https://github.com/deephaven-examples/deephaven-python-packaging/tree/main/my_dh_cli) in the example repository. Here is a minimal package layout:

```
my_dh_cli/
├── src/
│   └── my_dh_cli/
│       ├── __init__.py
│       ├── __main__.py
│       └── cli.py
└── pyproject.toml
```

The package metadata declares the runtime dependencies and the installed command. The examples use [Click](https://click.palletsprojects.com/) for argument parsing, so it is listed alongside `deephaven-server`. The `[project.scripts]` table defines a [console script entry point](https://setuptools.pypa.io/en/latest/userguide/entry_point.html#console-scripts); `pip` generates an executable with the given name when the package is installed. The entry point must name a callable that exists in the package:

```toml
[project]
name = "my-dh-cli"
version = "0.1.0"
description = "A command-line Deephaven CSV query"
requires-python = ">=3.9"
dependencies = ["deephaven-server", "click"]

[project.scripts]
my-dh-query = "my_dh_cli.cli:main"
```

Python imports the package before it resolves the console-script entry point, so `__init__.py` and `cli.py` must not import `deephaven` at module scope; if they did, every command in the package would fail at launch. Keep `__init__.py` empty or limited to imports that do not depend on Deephaven, and in `cli.py` import `Server` and `deephaven` inside `main()`, after argument parsing:

```python skip-test
import click


@click.command()
@click.argument("input_file", type=click.Path(exists=True))
@click.option(
    "--port", default=10000, show_default=True, help="Port for the embedded server"
)
def main(input_file: str, port: int) -> None:
    """Read a CSV file and add a computed column."""
    from deephaven_server import Server

    server = Server(port=port)
    server.start()

    from deephaven import read_csv

    table = read_csv(input_file)
    result = table.update(["DoubleScore = Score * 2"])
    click.echo(f"Processed {result.size} rows")
```

`Server` itself can be imported at any time; only `deephaven` has to wait for `server.start()`. The process owns the server and JVM for as long as it runs. If you want a command that works against a server you already have running, see [Package a remote Deephaven client](#package-a-remote-deephaven-client) instead.

> [!NOTE]
> A package that provides both a library and commands has to be more careful, because the library's own modules import `deephaven`. The repository's [`my_dh_toolkit`](https://github.com/deephaven-examples/deephaven-python-packaging/tree/main/my_dh_toolkit) shows that case: its `__init__.py` imports nothing, its commands import the library modules only after starting the server, and Python users import from the submodules (`my_dh_toolkit.queries`) rather than from the package.

Add a [`__main__.py`](https://docs.python.org/3/library/__main__.html#main-py-in-python-packages) if you also want to support `python -m my_dh_cli`. This is useful when the package is installed but its console script is not on the `PATH`, or when you want to be explicit about which Python interpreter runs the command:

```python skip-test
from my_dh_cli.cli import main

if __name__ == "__main__":
    main()
```

Once the package is installed, both launchers run the same `main()` function and accept the same arguments:

```bash
my-dh-query data/sample.csv
python -m my_dh_cli data/sample.csv
```

Each process that runs this command starts its own embedded server, which means the command carries JVM startup time and memory cost on every invocation. If another process already uses the selected port, such as a Deephaven server running in Docker, the embedded server fails to start because it cannot bind that port; pass a free port with `--port`.

## Package a remote Deephaven client

Use `pydeephaven` when the program should connect to a Deephaven server that is already running. This is a regular Python client application: it does not need `deephaven-server`, a local JVM, or a startup step before imports. The server runs elsewhere, so many client processes can connect to it at once, and the client process stays lightweight.

Because the client has no server to start, the embedded-server import rule does not apply. `pydeephaven` can be imported at module scope, including in `__init__.py`, and the package can be structured like any other Python project that talks to a network service.

The trade-off is that the client API is not the same as the server-side `deephaven` API. Client code works with table handles that refer to tables on the server, and operations on those handles are sent to the server for execution. Most common table operations are available, but code written against `deephaven` does not run unchanged against `pydeephaven`. One difference comes up in the examples below: the server cannot read files from the client's machine, so local data is uploaded as a [pyarrow](https://arrow.apache.org/docs/python/) table. Others are smaller, such as reading column names from `table.schema.names` rather than `table.columns`.

The examples connect with anonymous authentication. A Deephaven server started with Docker or `pip` uses a [pre-shared key](../authentication/auth-psk.md) by default; either start it with [anonymous authentication](../authentication/auth-anon.md) enabled, or pass the server's key to `Session` as shown in the client command below.

### Client library

A client library packages functions that operate on tables through a [`Session`](/core/client-api/python/code/pydeephaven.session.html#pydeephaven.session.Session). Pass the session in from the caller rather than creating one inside the library; this leaves connection details, authentication, and the session's lifetime under the calling program's control, and it lets the same functions be used against different servers. The full example is [`my_dh_client_library`](https://github.com/deephaven-examples/deephaven-python-packaging/tree/main/my_dh_client_library) in the example repository.

The `[project]` table is almost identical to the embedded-server library's, except that it declares `pydeephaven` instead of `deephaven-server`. It also declares `pyarrow`, which the library imports directly to read CSV files. `pydeephaven` depends on `pyarrow` already, but a package should declare what it imports rather than rely on a transitive dependency:

```toml
[project]
name = "my-dh-client-library"
version = "0.1.0"
description = "Reusable functions for a remote Deephaven server"
requires-python = ">=3.9"
dependencies = ["pydeephaven", "pyarrow"]
```

The library's functions take and return client-side [`Table`](/core/client-api/python/code/pydeephaven.table.html#pydeephaven.table.Table) handles. `src/my_dh_client_library/utils.py` holds `upload_csv`, which gets local data onto the server:

```python skip-test
import pyarrow.csv as pacsv
from pydeephaven import Session, Table


def upload_csv(session: Session, path: str) -> Table:
    """Read a local CSV file and upload it to the server as a table."""
    return session.import_table(pacsv.read_csv(path))
```

`src/my_dh_client_library/queries.py` holds the query functions. The query string in `filter_by_threshold` is the same one the embedded-server library used, but here the operation runs on the server and only the handle is returned to the client. `publish` gives a table a name on the server:

```python skip-test
from pydeephaven import Session, Table


def filter_by_threshold(table: Table, column: str, threshold: float) -> Table:
    """Keep rows where column > threshold."""
    return table.where([f"{column} > {threshold}"])


def publish(session: Session, name: str, table: Table) -> None:
    """Bind a table under a name so it is visible in the server's IDE and to other clients."""
    session.bind_table(name, table)
```

Because `pydeephaven` can be imported at module scope, `__init__.py` can re-export the public functions so that callers import them directly from the package name:

```python skip-test
from my_dh_client_library.queries import filter_by_threshold, publish
from my_dh_client_library.utils import upload_csv

__all__ = ["filter_by_threshold", "publish", "upload_csv"]
```

A calling program creates the `Session`, passes it to the library functions, and closes it when finished. Nothing has to start before the imports; the only requirement is a server to connect to:

```python skip-test
from pydeephaven import Session

from my_dh_client_library import filter_by_threshold, publish, upload_csv

with Session(host="localhost", port=10000) as session:
    scores = upload_csv(session, "data/sample.csv")
    high_scores = filter_by_threshold(scores, "Score", 75.0)
    publish(session, "high_scores", high_scores)
```

`high_scores` is now bound on the server, so it appears in the IDE and other clients can open it with `session.open_table("high_scores")`. The server's script scope holds the binding, so the table stays available after the session that created it closes, until the name is reassigned or deleted on the server. To bring rows back into the client process, call `high_scores.to_arrow()`.

### Client command

A client package can expose a command in the same way as an embedded-server package, but it depends on `pydeephaven` and uses `pydeephaven.Session`. The full example is [`my_dh_client`](https://github.com/deephaven-examples/deephaven-python-packaging/tree/main/my_dh_client) in the example repository. The layout is the same as the embedded-server command's, and `__main__.py` is the same two lines with the package name changed:

```
my_dh_client/
├── src/
│   └── my_dh_client/
│       ├── __init__.py
│       ├── __main__.py
│       └── cli.py
└── pyproject.toml
```

The `pyproject.toml` declares the dependencies and the console script in the same way as the embedded-server command:

```toml
[project]
name = "my-dh-client"
version = "0.1.0"
description = "A command-line Deephaven client"
requires-python = ">=3.9"
dependencies = ["pydeephaven", "pyarrow", "click"]

[project.scripts]
my-dh-client = "my_dh_client.cli:main"
```

The command below does the same work as the embedded-server command, but on a server it connects to: it uploads a CSV file, adds a column, and binds the result under a name. The name defaults to the file's stem and must be a valid Python identifier. The command checks the name before it connects and exits with a usage error if the name is invalid, so a file such as `my-scores.csv` needs `--name` to choose a different one. Unlike the embedded-server command, it imports `pydeephaven` at module scope and has no startup step. It uses anonymous authentication by default. For another authentication method, pass its name with `--auth-type`; provide any required token through the `DH_AUTH_TOKEN` environment variable rather than a command-line argument so it does not appear in shell history or process listings.

```python skip-test
import os
from pathlib import Path
from typing import Optional

import click
import pyarrow.csv as pacsv
from pydeephaven import Session


@click.command()
@click.argument("input_file", type=click.Path(exists=True, dir_okay=False))
@click.option(
    "--host", default="localhost", show_default=True, help="Deephaven server host"
)
@click.option("--port", default=10000, show_default=True, help="Deephaven server port")
@click.option(
    "--auth-type", default="Anonymous", show_default=True, help="Authentication type"
)
@click.option(
    "--name", default=None, help="Name to bind the result under [default: file stem]"
)
def main(
    input_file: str, host: str, port: int, auth_type: str, name: Optional[str]
) -> None:
    """Upload a CSV file to a running Deephaven server and process it there."""
    name = name or Path(input_file).stem
    if not name.isidentifier():
        raise click.BadParameter(
            f"'{name}' is not a valid Python identifier", param_hint="--name"
        )

    with Session(
        host=host,
        port=port,
        auth_type=auth_type,
        auth_token=os.environ.get("DH_AUTH_TOKEN", ""),
    ) as session:
        table = session.import_table(pacsv.read_csv(input_file))
        result = table.update(["DoubleScore = Score * 2"])
        session.bind_table(name, result)

    click.echo(f"Bound result table '{name}' on {host}:{port}")
```

`Session` is a context manager, so the client connection closes when the command finishes. The result table lives on the remote server, not in the client process, and because it is bound in the server's script scope, it remains available under its name after the command exits. Running the command several times does not start additional servers, so there is no port to choose and no conflict with a server already using it. The command starts quickly because there is no JVM to launch, which makes the client model a good fit for small utilities that are run often.

Once installed, run it against a server on the default host and port, or point it elsewhere:

```bash
my-dh-client data/sample.csv
my-dh-client data/sample.csv --host dh.example.com --port 10000
```

For a server that uses a pre-shared key, set `DH_AUTH_TOKEN` in the environment without typing the key into a command. For example, read it from a file that is not checked into version control, or use `read -s` to enter it at a hidden prompt. Then pass the handler's class name:

```bash
read -s DH_AUTH_TOKEN && export DH_AUTH_TOKEN
my-dh-client data/sample.csv --auth-type io.deephaven.authentication.psk.PskAuthenticationHandler
```

The install and distribution steps that follow apply equally to embedded-server and client packages. Nothing about the build process depends on which Deephaven package the project uses.

## Install and distribute a project

The commands in this section are run from the project directory, the one that contains `pyproject.toml`. They work the same way for every example in this guide.

During development, install from the project directory in [editable mode](https://pip.pypa.io/en/stable/topics/local-project-installs/#editable-installs). This makes changes in the source tree available without reinstalling, which is convenient while iterating on a command or library:

```bash
python -m pip install -e .
```

To install a project normally from its source directory, use `python -m pip install .`. This copies the package into the environment's `site-packages`, so later source changes are not picked up until you reinstall.

Projects often have dependencies that only some users need, such as plotting libraries or test tooling. Rather than requiring them for everyone, declare them as **extras** in [`[project.optional-dependencies]`](https://packaging.python.org/en/latest/guides/writing-pyproject-toml/#dependencies-optional-dependencies) in `pyproject.toml`:

```toml
[project.optional-dependencies]
visualization = ["matplotlib>=3.7"]
dev = ["pytest>=7"]
```

Users then request extras by name in square brackets after the project. When installing from the source directory, the project is `.`, with or without `-e`. Quote the requirement so shells such as zsh do not treat the square brackets as filename patterns:

```bash
python -m pip install ".[visualization]"
python -m pip install -e ".[visualization,dev]"
```

The same syntax works for a published project, for example `python -m pip install "my-dh-library[visualization]"`, in which case `pip` resolves the distribution from its configured package index rather than the local directory.

To distribute the project, use [`build`](https://build.pypa.io/en/stable/) to produce a wheel and source distribution from the project directory. The first command installs `build` itself; the second runs it:

```bash
python -m pip install --upgrade build
python -m build
```

The generated files are placed in `dist/`. A wheel can be installed directly with `python -m pip install dist/my_project-0.1.0-py3-none-any.whl`, copied to another machine, or hosted on an internal package index. For many teams, sharing wheels this way is all that is needed. Publishing to PyPI requires additional account, package-name, and upload configuration; follow the Python Packaging User Guide's [tutorial on packaging and distributing projects](https://packaging.python.org/en/latest/tutorials/packaging-projects/) for those steps.

## Further reading

This guide covers the parts of Python packaging that matter most for Deephaven projects. The resources below go deeper on the tooling, the specifications, and the Deephaven packages themselves.

Official Python packaging documentation:

- [Python Packaging User Guide](https://packaging.python.org/en/latest/), including the [packaging projects tutorial](https://packaging.python.org/en/latest/tutorials/packaging-projects/) and [Writing your `pyproject.toml`](https://packaging.python.org/en/latest/guides/writing-pyproject-toml/)
- [`pyproject.toml` specification](https://packaging.python.org/en/latest/specifications/pyproject-toml/), [entry points specification](https://packaging.python.org/en/latest/specifications/entry-points/), and [dependency specifiers](https://packaging.python.org/en/latest/specifications/dependency-specifiers/)
- [src layout vs flat layout](https://packaging.python.org/en/latest/discussions/src-layout-vs-flat-layout/)
- [Setuptools user guide](https://setuptools.pypa.io/en/latest/userguide/index.html): [`pyproject.toml` configuration](https://setuptools.pypa.io/en/latest/userguide/pyproject_config.html), [package discovery](https://setuptools.pypa.io/en/latest/userguide/package_discovery.html), and [entry points](https://setuptools.pypa.io/en/latest/userguide/entry_point.html)
- [`pip` local project installs](https://pip.pypa.io/en/stable/topics/local-project-installs/) and the [`build` frontend](https://build.pypa.io/en/stable/)
- [`__main__.py` in Python packages](https://docs.python.org/3/library/__main__.html#main-py-in-python-packages)

Community guides to `pyproject.toml`:

- [Real Python: How to manage Python projects with `pyproject.toml`](https://realpython.com/python-pyproject-toml/)
- [PyDevTools `pyproject.toml` reference](https://pydevtools.com/handbook/reference/pyproject.toml/)

Deephaven documentation:

- [Choose the right Deephaven Python packages](../../reference/cheat-sheets/choose-python-packages.md)
- [Install and use Python packages in Deephaven](../install-and-use-python-packages.md)
- [Python Client Quickstart](../../getting-started/pyclient-quickstart.md)
- [`pydeephaven` API reference](/core/client-api/python/)
- [deephaven-python-packaging](https://github.com/deephaven-examples/deephaven-python-packaging), the example repository this guide draws from
