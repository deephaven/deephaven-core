---
title: Package a Deephaven Python project
sidebar_label: Package Python projects
---

Python packaging turns a project containing Python code into a distribution that can be installed with `pip`. A distribution can provide an importable library, command-line tools, or both. This guide shows how to package Python projects that use Deephaven.

The Deephaven Python packages support two different ways to work with a server:

- **Embedded server:** `deephaven-server` starts a Deephaven server and its JVM inside the Python process. The `deephaven` API is available in that same process after the server starts.
- **Remote client:** `pydeephaven` connects a Python process to a Deephaven server that is already running. The client does not start a server or provide the server-side `deephaven` API.

Choose the model that fits how the program will run. Dependencies and initialization differ between them.

## Python packaging terms

A Python **project** is the source code and configuration you develop. Building it produces a **distribution package**, usually a wheel (`.whl`) or source archive, which users install with `pip`. An **import package** is the code users import in Python. Its name can differ from the distribution name: for example, a project distributed as `my-dh-cli` can provide the import package `my_dh_cli`.

The standard project configuration file is [`pyproject.toml`](https://packaging.python.org/en/latest/guides/writing-pyproject-toml/). The examples use the [src layout](https://packaging.python.org/en/latest/discussions/src-layout-vs-flat-layout/), which keeps importable code in `src/`:

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

Use `deephaven-server` when the Python program needs to start a Deephaven server itself. The server and the code using `deephaven` must run in the same Python process.

### Library package

A library package provides functions for another program to import. It does not need to start a server itself; the importing program must start one before importing code that depends on `deephaven`.

For a library that uses the embedded server, declare `deephaven-server` as a dependency. It supplies the `deephaven` module through its `deephaven-core` dependency. The following configuration uses setuptools and discovers the import package under `src/`:

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

Set the Python version and dependency constraints to versions you test and support. A lower bound such as `deephaven-server>=0.35` declares compatibility; it does not lock the environment to a repeatable set of versions.

### Command-line package

A command-line program can start an embedded server before it imports the `deephaven` API. Here is a minimal package layout:

```
my_dh_cli/
├── src/
│   └── my_dh_cli/
│       ├── __init__.py
│       ├── __main__.py
│       └── cli.py
└── pyproject.toml
```

The package metadata declares both the runtime dependency and the installed command. The entry point must name a callable that exists in the package:

```toml
[build-system]
requires = ["setuptools>=61"]
build-backend = "setuptools.build_meta"

[project]
name = "my-dh-cli"
version = "0.1.0"
description = "A command-line Deephaven CSV query"
requires-python = ">=3.9"
dependencies = ["deephaven-server"]

[project.scripts]
my-dh-query = "my_dh_cli.cli:main"

[tool.setuptools.packages.find]
where = ["src"]
```

Keep `__init__.py` empty or limited to imports that do not depend on Deephaven. Python imports the package before it resolves the console-script entry point, so an import of `deephaven` at package or command-module scope would happen before the server starts.

In `cli.py`, parse arguments first, start the server, and only then import Deephaven modules:

```python skip-test
import argparse
from pathlib import Path


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("csv_file", type=Path)
    parser.add_argument("--port", type=int, default=10000)
    args = parser.parse_args()

    from deephaven_server import Server

    server = Server(port=args.port)
    server.start()

    from deephaven import read_csv

    table = read_csv(str(args.csv_file))
    print(table)
```

The import of `Server` is safe before startup; import the `deephaven` API only after `server.start()`. The process owns the server and JVM, so this example is a standalone program rather than a client of a separate server.

Add `__main__.py` if you also want to support `python -m my_dh_cli`:

```python skip-test
from my_dh_cli.cli import main

if __name__ == "__main__":
    main()
```

Once installed, use either launcher:

```bash
my-dh-query data/sample.csv
python -m my_dh_cli data/sample.csv
```

Each process that runs this command starts its own embedded server. If another process already uses the selected port, pass a free port with `--port`.

## Package a remote Deephaven client

Use [`pydeephaven`](https://pypi.org/project/pydeephaven/) when the program should connect to an existing Deephaven server. This is a regular Python client application: it does not need `deephaven-server`, a local JVM, or an embedded-server startup step.

A client package can expose a command in the same way as an embedded-server package, but it depends on `pydeephaven` and uses `pydeephaven.Session`. The example's source file is `src/my_dh_client/cli.py`:

```
my_dh_client/
├── src/
│   └── my_dh_client/
│       ├── __init__.py
│       └── cli.py
└── pyproject.toml
```

```toml
[build-system]
requires = ["setuptools>=61"]
build-backend = "setuptools.build_meta"

[project]
name = "my-dh-client"
version = "0.1.0"
description = "A command-line Deephaven client"
requires-python = ">=3.9"
dependencies = ["pydeephaven"]

[project.scripts]
my-dh-client = "my_dh_client.cli:main"

[tool.setuptools.packages.find]
where = ["src"]
```

The command below connects to the given server, creates a table there, and binds it under a name. It supports anonymous authentication by default. For another authentication method, pass its name with `--auth-type`; provide any required token through the `DH_AUTH_TOKEN` environment variable rather than a command-line argument.

```python skip-test
import argparse
import os

from pydeephaven import Session


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="localhost")
    parser.add_argument("--port", type=int, default=10000)
    parser.add_argument("--auth-type", default="Anonymous")
    args = parser.parse_args()

    with Session(
        host=args.host,
        port=args.port,
        auth_type=args.auth_type,
        auth_token=os.environ.get("DH_AUTH_TOKEN", ""),
    ) as session:
        table = session.time_table("PT1s").update(["X = ii"])
        session.bind_table("packaging_example", table)
```

`Session` is a context manager, so the client connection closes when the command finishes. The created table is on the remote server; its lifetime follows the server's normal session and export rules.

## Install and distribute a project

During development, install from the project directory in editable mode. This makes changes in the source tree available without reinstalling:

```bash
python -m pip install -e .
```

To install a project normally from its source directory, use `python -m pip install .`. To request optional dependency groups, quote the requirement so shells such as zsh do not treat square brackets as filename patterns:

```bash
python -m pip install "my-dh-library[visualization]"
python -m pip install "my-dh-library[visualization,dev]"
```

Optional dependency groups are declared in `[project.optional-dependencies]` in `pyproject.toml`. For example:

```toml
[project.optional-dependencies]
visualization = ["matplotlib>=3.7"]
dev = ["pytest>=7"]
```

To distribute the project, build a wheel and source archive from its project directory:

```bash
python -m pip install --upgrade build
python -m build
```

The generated files are placed in `dist/`. A wheel can be installed directly with `python -m pip install dist/my_project-0.1.0-py3-none-any.whl` or shared with other users. Publishing to PyPI requires additional account, package-name, and upload configuration; follow the Python Packaging User Guide's [tutorial on packaging and distributing projects](https://packaging.python.org/en/latest/tutorials/packaging-projects/) for those steps.

## Further reading

- [Writing a `pyproject.toml`](https://packaging.python.org/en/latest/guides/writing-pyproject-toml/) and the [`pyproject.toml` specification](https://packaging.python.org/en/latest/specifications/pyproject-toml/)
- [Setuptools documentation](https://setuptools.pypa.io/)
- [Python Packaging User Guide: packaging and distributing projects](https://packaging.python.org/en/latest/tutorials/packaging-projects/)
- [Install and use Python packages in Deephaven](../install-and-use-python-packages.md)
- [Choose the right Deephaven Python packages](../../reference/cheat-sheets/choose-python-packages.md)
- [Python Client Quickstart](../../getting-started/pyclient-quickstart.md)
- [Real Python: `pyproject.toml`](https://realpython.com/python-pyproject-toml/)
- [PyDevTools `pyproject.toml` reference](https://pydevtools.com/handbook/reference/pyproject.toml/)
