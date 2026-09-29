---
title: Packaging Python projects that use Deephaven
sidebar_label: Python project packaging
---

This guide is about packaging your own Python project for installation and distribution—not installing Python packages into a running Deephaven server. For the latter, see [Install and use Python packages](../install-and-use-python-packages.md).

Python's standard packaging tools let you describe a project's metadata and dependencies in `pyproject.toml`, build a wheel, and share it with other environments. A project that uses Deephaven can package reusable Python code, command-line applications, or both. The right dependencies and startup steps depend on whether the project embeds a Deephaven server or connects to one with the Python client.

The examples cover four common project shapes: an importable library, a command-line application using the embedded server, a project with both library code and commands, and a client application using `pydeephaven`.

## Choose how your project uses Deephaven

There are two distinct Python APIs and runtime models:

- The server-side `deephaven` API runs in a Python process that embeds a Deephaven server. The `deephaven-server` distribution provides the server startup API and depends on `deephaven-core`, which provides the `deephaven` module. Start the server in the same process before importing and using `deephaven`.
- The `pydeephaven` client API runs in a separate Python process and connects to an already-running Deephaven server over the network. The client process does not start an embedded server or import the server-side `deephaven` module. It needs the server address and, when configured, suitable authentication details.

These approaches can also be combined in a larger system, but the examples below keep their dependencies and startup requirements explicit. See the [Python client quickstart](../../getting-started/pyclient-quickstart.md) for more on connecting to a server with `pydeephaven`.

## Package structure

The examples use the **src layout**, which keeps importable code separate from project configuration and tests. The [Python Packaging User Guide](https://packaging.python.org/en/latest/discussions/src-layout-vs-flat-layout/) describes the tradeoffs between src and flat layouts:

```
my_dh_library/
├── src/
│   └── my_dh_library/
│       ├── __init__.py
│       ├── queries.py
│       └── utils.py
├── pyproject.toml
└── README.md
```

### Key components

- **`src/`** - Source directory containing the package code.
- **`my_dh_library/`** (under `src/`) - The Python import package. Its directory name is used in `import` statements.
- **`__init__.py`** - Makes the directory importable and can export a public API.
- **`pyproject.toml`** - Defines package metadata, dependencies, and entry points.
- **Module files** - Python files containing your functions and classes.

The package directory under `src/` determines the import name. With the layout above, users might write `from my_dh_library import ...`.

## Start an embedded server

For projects using the server-side `deephaven` API, start an embedded server in the Python process before importing `deephaven`. For example:

```python skip-test
from deephaven_server import Server

# Start the embedded server in this process
server = Server(port=10000, jvm_args=["-Xmx4g"])
server.start()

# Import and use Deephaven only after starting the server
from deephaven import empty_table

table = empty_table(3).update("X = ii")
```

This requirement applies to the embedded server API, not to the `pydeephaven` client. Keep the following distinctions in mind:

- The embedded server and its Python API run in the same process. A server started by another process—for example, in a different terminal—does not initialize this process.
- A command-line application that uses the embedded API must start its own server (see [Command-line application](#command-line-application)). A reusable library can instead expect its caller to have started the server and document that requirement.
- `jvm_args=["-Xmx4g"]` sets the embedded JVM's maximum heap to 4 GB in this example; choose a value appropriate for the workload and available memory.

> [!NOTE]
> The embedded-server example binds to port 10000. If another process already uses that port (for example, a Deephaven server running in Docker), the server fails to start with `Address already in use`. Change the `port` value to a free port.

### Keep `__init__.py` free of Deephaven imports

For a command-line application using the embedded API, the order of imports matters. When a command such as `my-dh-toolkit-query` starts, Python imports the package's `__init__.py` before calling the command function. If that initializer imports a module that imports `deephaven`, the import happens before the command can start its server and the command fails.

To avoid that startup-order problem:

- Keep `__init__.py` and command modules free of imports that reach `deephaven` until after the command starts its server.
- Import `deephaven` and Deephaven-dependent modules inside the function that runs after startup.
- A library that uses the embedded API may re-export its functions from `__init__.py`, provided its documented callers start the server before importing or calling those functions.

## Packaging scenarios

The first three scenarios use the embedded server API and assume that the server has been started as described in [Start an embedded server](#start-an-embedded-server). The fourth scenario uses the client API and connects to a server that is already running elsewhere.

### Library-only package

A library-only project provides reusable modules for other Python code to import; it does not install a command-line program. Callers must start the embedded server before importing code that uses the `deephaven` API.

**Structure:**

```
my_dh_library/
├── src/
│   └── my_dh_library/
│       ├── __init__.py
│       ├── queries.py
│       └── utils.py
├── pyproject.toml
└── README.md
```

**Usage:**

```python skip-test
from my_dh_library.queries import filter_by_threshold, add_computed_columns
from deephaven import read_csv

data = read_csv("data/sample.csv")
filtered = filter_by_threshold(data, "Score", 75.0)
```

This usage sketch assumes that the library has been installed and provides the named function. The sample CSV path represents input data in the user's environment.

**Use when:**

- Creating reusable utilities for other projects.
- You don't need a command-line interface.
- Code will be imported, not executed directly.

### Command-line application

An installable command-line application exposes one or more commands for users to run from a shell. This example starts its own embedded server, so it can run as a standalone command. The `pyproject.toml` file defines the command entry point, and the `__main__.py` module also supports running the package with `python -m`.

**Structure:**

```
my_dh_cli/
├── src/
│   └── my_dh_cli/
│       ├── __init__.py
│       ├── __main__.py
│       └── cli.py
├── pyproject.toml
└── README.md
```

**Usage:**

```bash
my-dh-query input.csv --verbose
```

Replace `input.csv` with the path to a CSV file available to the command.

**Use when:**

- Building command-line tools for data processing.
- Users run the command from a shell without writing a Python script to invoke it.
- You don't need to expose library code to other projects.

### Combined package

Some projects need both an importable library and shell commands. A combined project can have commands call the same query and utility functions exposed to Python callers. Each command has its own module and a `main()` function; for example, `my-dh-toolkit-query` can point to `my_dh_toolkit.query:main`.

**Structure:**

```
my_dh_toolkit/
├── src/
│   └── my_dh_toolkit/
│       ├── __init__.py
│       ├── __main__.py
│       ├── query.py
│       ├── process.py
│       ├── queries.py
│       └── utils.py
├── pyproject.toml
└── README.md
```

**Usage:**

```python skip-test
# As a library
from my_dh_toolkit.queries import filter_by_threshold
from deephaven import read_csv

data = read_csv("data/sample.csv")
filtered = filter_by_threshold(data, "Score", 75.0)
```

```bash
# As CLI commands
my-dh-toolkit-query data/sample.csv --verbose
my-dh-toolkit-process data/batch --output output --verbose
```

These command examples assume the package's command entry points have been installed and that the input paths exist in the current environment.

**Use when:**

- You need both library and CLI functionality.
- You want to provide multiple interfaces to the same code.
- Library functions are useful independently.

### Client application using `pydeephaven`

A Python client application can be packaged independently of the Deephaven server. Declare `pydeephaven` as a project dependency, then connect to a server that is already running. For example, the following project metadata and function use the client package; they do not start an embedded server:

```toml
[build-system]
requires = ["setuptools>=61.0"]
build-backend = "setuptools.build_meta"

[project]
name = "my-dh-client"
version = "0.1.0"
requires-python = ">=3.9"
dependencies = ["pydeephaven"]

[tool.setuptools.packages.find]
where = ["src"]
```

The `src/my_dh_client/__init__.py` module can define a client function such as:

```python skip-test
from pydeephaven import Session


def create_table(host: str = "localhost", port: int = 10000) -> None:
    with Session(host=host, port=port) as session:
        table = session.time_table(period=1_000_000_000).update(["X = ii"])
        session.bind_table("my_table", table)
```

The caller supplies the server address and any required authentication configuration. Keep credentials outside the package source, for example in environment-specific configuration. For connection options and client operations, see the [Python client quickstart](../../getting-started/pyclient-quickstart.md).

## Configure `pyproject.toml`

`pyproject.toml` is the standard place for project metadata, build-system requirements, and configuration for tools such as setuptools. The [Python Packaging User Guide](https://packaging.python.org/en/latest/guides/writing-pyproject-toml/) explains how to write the file; the [PyPA specification](https://packaging.python.org/en/latest/specifications/pyproject-toml/) defines its standard fields.

### Configuration options

The following `pyproject.toml` excerpt configures an embedded-server library:

```toml
[build-system]
requires = ["setuptools>=61.0", "wheel"]
build-backend = "setuptools.build_meta"

[project]
name = "my-dh-library"
version = "0.1.0"
description = "Reusable Deephaven query functions"
readme = "README.md"
requires-python = ">=3.9"
dependencies = [
  # deephaven-server also provides the deephaven module (through its deephaven-core dependency).
  "deephaven-server>=0.35.0",
]

[tool.setuptools.packages.find]
where = ["src"]
```

### Key sections

- **`[build-system]`** declares the build backend and the packages needed to build the project.
- **`[project]`** contains standardized project metadata and runtime dependencies.
- **`name`** is the distribution name users pass to `pip install`; it does not have to match the Python import package name.
- **`dependencies`** lists runtime distributions that installers should install with the project.
- **`[tool.setuptools.packages.find]`** is setuptools-specific configuration that tells it to discover import packages under `src/`.

To install shell commands with a package, add a `[project.scripts]` table. Each entry maps a command name to an importable Python object, usually a `main` function:

```toml
[project.scripts]
my-dh-query = "my_dh_cli.cli:main"
```

This entry installs a `my-dh-query` command that calls `main` in the `my_dh_cli.cli` module. A project can define multiple commands in `[project.scripts]`, each mapped to a callable in an importable module.

## Manage dependencies

Declare the packages your project needs at runtime in `[project].dependencies`. For example, an embedded-server application might require Deephaven, Click for its command-line interface, and pandas for data processing:

```toml
[project]
dependencies = [
  "deephaven-server>=0.35.0",
  "click>=8.0.0",
  "pandas>=2.0.0",
]
```

For the embedded API, declaring `deephaven-server` is sufficient: it depends on a matching version of `deephaven-core`, which provides the importable `deephaven` module. A client application should depend on `pydeephaven` instead, as shown in the [client example](#client-application-using-pydeephaven).

### Version constraints

Version specifiers describe which versions your project supports. For example:

- `>=0.35.0` - Minimum version (0.35.0 or higher)
- `>=2.0.0,<3.0.0` - Version range (2.x only)
- `~=1.24.0` - Compatible release (>=1.24.0, <1.25.0)
- `==1.0.0` - Exact version (useful for fully pinned applications, but usually too strict for libraries)

An open lower bound such as `deephaven-server>=0.35.0` is a compatibility constraint, not a reproducible lock: it permits newer releases as they become available. Libraries generally use supported version ranges, while applications that need repeatable deployments should also lock their complete environment with a lock file or an equivalent deployment-specific pinning process.

### Optional dependencies

Define optional feature sets that users can install separately. Quote the requirement so shells that treat square brackets as pattern characters pass it unchanged:

```toml
[project.optional-dependencies]
visualization = [
  "matplotlib>=3.7.0",
  "seaborn>=0.12.0",
]
dev = [
  "pytest>=7.0.0",
  "black>=23.0.0",
]
```

Users can request one or more optional feature sets when installing the project:

```bash
pip install 'my-dh-library[visualization]'
pip install 'my-dh-library[visualization,dev]'
```

## Install and distribute

During development, install the project in editable mode from its root directory. This makes changes to the source package available without reinstalling after each edit:

```bash
cd my_dh_library
pip install -e .
```

For a regular local installation, omit `-e`:

```bash
pip install .
```

After installation, use the project according to its runtime model: import a library after meeting its documented server requirements, run an installed command, or connect to an existing server with the `pydeephaven` client.

To build a wheel that can be installed or shared with another environment, install the `build` frontend in your build environment and run it from the project root:

```bash
cd my_dh_library
pip install build
python -m build
```

The resulting `.whl` file is written to `dist/`. Install it locally with pip:

```bash
pip install dist/my_dh_library-0.1.0-py3-none-any.whl
```

You can also share the wheel directly with users or publish it to a package index. Publishing involves account and upload configuration, so follow the [Python Packaging User Guide's publishing walkthrough](https://packaging.python.org/en/latest/tutorials/packaging-projects/#uploading-the-distribution-archives) rather than treating an upload command as a complete release process.

## Best practices

### Package structure

- Prefer the src layout for projects like these examples.
- Use a clear, lowercase distribution name; hyphens are common. Use valid Python identifiers, often lowercase with underscores, for import package names.
- Keep the distribution name and import package name distinct in your documentation if they differ.
- Include `__init__.py` for regular packages; use namespace packages only when that packaging model is intentional.
- In embedded-server projects with commands, defer imports of `deephaven` until after the command starts the server.

### Dependencies

- Specify compatible version ranges for Deephaven and other important dependencies.
- Group optional dependencies by feature or development task.
- Document system-level requirements, such as the Java version, when they apply.

### Documentation

- Include a README with installation and usage instructions.
- Document public functions, classes, and required server setup.
- Include sample data when it is needed to try the package.

## Apply a packaging pattern to your project

Choose a project shape based on how users will consume your code, then adapt the relevant layout and metadata from this guide:

1. Decide whether you need an importable library, an embedded-server command, both, or a client that connects to an existing server.
2. Create a `src/` package and update `pyproject.toml` with your distribution metadata and the dependencies for that runtime model.
3. Add `[project.scripts]` only if you want pip to install shell commands. For embedded-server commands, start the server before importing `deephaven`.
4. Build a wheel, install it in a clean environment, and test the documented import or command against the intended server setup.

## Related documentation

- [Install and use Python packages](../install-and-use-python-packages.md)
- [Use the Deephaven Python package](../deephaven-python-package.md)
- [Python client quickstart](../../getting-started/pyclient-quickstart.md)
- [Writing your `pyproject.toml`](https://packaging.python.org/en/latest/guides/writing-pyproject-toml/)
- [`pyproject.toml` specification](https://packaging.python.org/en/latest/specifications/pyproject-toml/)
- [Packaging Python projects](https://packaging.python.org/en/latest/tutorials/packaging-projects/)
- [src layout vs flat layout](https://packaging.python.org/en/latest/discussions/src-layout-vs-flat-layout/)
- [Creating and packaging command-line tools](https://packaging.python.org/en/latest/guides/creating-command-line-tools/)
- [Setuptools documentation](https://setuptools.pypa.io/)
- [A practical guide to `pyproject.toml`](https://realpython.com/python-pyproject-toml/)
- [The `pyproject.toml` handbook reference](https://pydevtools.com/handbook/reference/pyproject.toml/)
- [Click documentation](https://click.palletsprojects.com/)
