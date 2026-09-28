---
title: Packaging custom code and dependencies
sidebar_label: Python packaging
---

[Python packaging](https://packaging.python.org/en/latest/guides/writing-pyproject-toml/) enables you to create distributable packages containing custom code, command line tools, and managed dependencies. Deephaven's own packages are pip-installable, so a package that depends on them can be built and installed with the standard Python tooling. This guide walks through the concepts and patterns for packaging Deephaven-based Python projects.

Python packaging with [`pyproject.toml`](https://packaging.python.org/en/latest/guides/writing-pyproject-toml/) provides:

- **Reusable libraries** - Package query functions and utilities for import by other projects.
- **Command line tools** - Build executable scripts with entry point definitions.
- **Dependency management** - Automatically install Deephaven and required packages, with explicit compatibility constraints.
- **Distribution** - Share code as wheel archives via PyPI or direct distribution.

## Example repository

The examples in this guide use the [deephaven-python-packaging](https://github.com/deephaven-examples/deephaven-python-packaging) repository. It demonstrates three complete packaging scenarios with working code, sample data, and a README for each package.

To explore the examples, clone the repository:

```bash
git clone https://github.com/deephaven-examples/deephaven-python-packaging.git
cd deephaven-python-packaging
```

The repository contains three example packages:

- [`my_dh_library/`](https://github.com/deephaven-examples/deephaven-python-packaging/tree/main/my_dh_library) - Library-only package with reusable query functions.
- [`my_dh_cli/`](https://github.com/deephaven-examples/deephaven-python-packaging/tree/main/my_dh_cli) - CLI-only package with a command line tool.
- [`my_dh_toolkit/`](https://github.com/deephaven-examples/deephaven-python-packaging/tree/main/my_dh_toolkit) - Combined package with both library and CLI functionality.

## Package structure

The example packages in this guide use the **src-layout** described in the Python Packaging Authority's [src layout vs flat layout discussion](https://packaging.python.org/en/latest/discussions/src-layout-vs-flat-layout/). This layout keeps source code separate from tests and configuration files:

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
- **`my_dh_library/`** (under `src/`) - The Python package. Its directory name is the name used in `import` statements.
- **`__init__.py`** - Makes the directory importable and can export a public API.
- **`pyproject.toml`** - Defines package metadata, dependencies, and entry points.
- **Module files** - Python files containing your functions and classes.

The package name under `src/` determines how users import your code. For example, with [`src/my_dh_library/`](https://github.com/deephaven-examples/deephaven-python-packaging/tree/main/my_dh_library/src/my_dh_library), users import via `from my_dh_library import ...`.

## Server initialization

Deephaven requires a running server before using any Deephaven functionality. The server must be initialized in the same Python process that uses Deephaven:

```python
from deephaven_server import Server

# Initialize and start the server
server = Server(port=10000, jvm_args=["-Xmx4g"])
server.start()

# Now you can import and use Deephaven
from deephaven import read_csv
data = read_csv("data/sample.csv")
```

### Key points

- Each Python process has its own JVM.
- Starting a server in one terminal doesn't help another terminal.
- Entry-point CLI commands should start their own server internally (see [CLI-only package](#cli-only-package)) so they work standalone; only functions imported directly need an already-running session.
- The examples size the JVM to 4 GB with `jvm_args=["-Xmx4g"]`; adjust this value to fit the workload.

> [!NOTE]
> The examples bind the server to port 10000. If another process already uses that port (for example, a Deephaven server running in Docker), the server fails to start with `Address already in use`. Change the `port` value to a free port.

### Keep `__init__.py` free of Deephaven imports

Because `deephaven` modules cannot be imported until a server is running, the order of imports matters in any package that defines a command. When a command such as `my-dh-toolkit-query` (defined in the toolkit's [`pyproject.toml`](https://github.com/deephaven-examples/deephaven-python-packaging/blob/main/my_dh_toolkit/pyproject.toml) and implemented in [`query.py`](https://github.com/deephaven-examples/deephaven-python-packaging/blob/main/my_dh_toolkit/src/my_dh_toolkit/query.py)) starts, Python imports the package ([`my_dh_toolkit/__init__.py`](https://github.com/deephaven-examples/deephaven-python-packaging/blob/main/my_dh_toolkit/src/my_dh_toolkit/__init__.py)) before the command function has a chance to start the server. If `__init__.py` imported a module that imports `deephaven`, every command in the package would fail at startup.

The rule that follows:

- In a package that defines commands, keep every module imported *before* the command starts its server free of imports that reach `deephaven`. In practice, that means `__init__.py` and the top level of command modules should stay import-light.
- Import `deephaven` and any Deephaven-dependent library submodules lazily, inside the function that runs after the server has started.
- A library-only package such as `my_dh_library` can safely re-export its functions from [`__init__.py`](https://github.com/deephaven-examples/deephaven-python-packaging/blob/main/my_dh_library/src/my_dh_library/__init__.py). It has no commands, so it is only ever imported after a server is running.

## Packaging scenarios

Different projects have different needs. The example repository demonstrates three common scenarios. The Python usage snippets below assume a running Deephaven server, as shown in [Server initialization](#server-initialization).

### Library-only package

Package reusable code without CLI tools. Other projects import your modules. See the example files in the repository: [`pyproject.toml`](https://github.com/deephaven-examples/deephaven-python-packaging/blob/main/my_dh_library/pyproject.toml), [`__init__.py`](https://github.com/deephaven-examples/deephaven-python-packaging/blob/main/my_dh_library/src/my_dh_library/__init__.py), [`queries.py`](https://github.com/deephaven-examples/deephaven-python-packaging/blob/main/my_dh_library/src/my_dh_library/queries.py), and [`utils.py`](https://github.com/deephaven-examples/deephaven-python-packaging/blob/main/my_dh_library/src/my_dh_library/utils.py).

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

```python
from my_dh_library.queries import filter_by_threshold, add_computed_columns
from deephaven import read_csv

data = read_csv("data/sample.csv")
filtered = filter_by_threshold(data, "Score", 75.0)
```

**Use when:**

- Creating reusable utilities for other projects.
- You don't need a command line interface.
- Code will be imported, not executed directly.

### CLI-only package

Package an executable command line tool without exposing library code. The command starts its own Deephaven server, so it runs as a standalone terminal command. See the example files in the repository: [`pyproject.toml`](https://github.com/deephaven-examples/deephaven-python-packaging/blob/main/my_dh_cli/pyproject.toml), [`cli.py`](https://github.com/deephaven-examples/deephaven-python-packaging/blob/main/my_dh_cli/src/my_dh_cli/cli.py), and [`__main__.py`](https://github.com/deephaven-examples/deephaven-python-packaging/blob/main/my_dh_cli/src/my_dh_cli/__main__.py).

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
my-dh-query data/sample.csv --verbose
```

**Use when:**

- Building command line tools for data processing.
- The tool is run from a terminal, with no Python code required from the user.
- You don't need to expose library code to other projects.

### Combined package

Package both reusable library code and command line tools. In `my_dh_toolkit`, the commands call the package's own library functions: `my-dh-toolkit-query` and `my-dh-toolkit-process` both use `validate_columns` and `add_computed_columns` from `my_dh_toolkit.utils` and `my_dh_toolkit.queries`. Python users and terminal users get two interfaces to one implementation. Each command lives in its own module, named after the command, and each module exposes the command as a function named `main()`: `my-dh-toolkit-query` comes from `my_dh_toolkit.query:main`, and `my-dh-toolkit-process` from `my_dh_toolkit.process:main`. See the example files in the repository: [`pyproject.toml`](https://github.com/deephaven-examples/deephaven-python-packaging/blob/main/my_dh_toolkit/pyproject.toml), [`__init__.py`](https://github.com/deephaven-examples/deephaven-python-packaging/blob/main/my_dh_toolkit/src/my_dh_toolkit/__init__.py), [`query.py`](https://github.com/deephaven-examples/deephaven-python-packaging/blob/main/my_dh_toolkit/src/my_dh_toolkit/query.py), and [`process.py`](https://github.com/deephaven-examples/deephaven-python-packaging/blob/main/my_dh_toolkit/src/my_dh_toolkit/process.py).

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

```python
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

**Use when:**

- You need both library and CLI functionality.
- You want to provide multiple interfaces to the same code.
- Library functions are useful independently.

## Configure `pyproject.toml`

The [`pyproject.toml`](https://packaging.python.org/en/latest/guides/writing-pyproject-toml/) file defines your package configuration.

### Configuration options

Here's a detailed breakdown of the library-only package's [`pyproject.toml`](https://github.com/deephaven-examples/deephaven-python-packaging/blob/main/my_dh_library/pyproject.toml):

```toml
[build-system]
requires = ["setuptools>=61.0", "wheel"]
build-backend = "setuptools.build_meta"

[project]
name = "my_dh_library"
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

- **`[build-system]`** - Specifies setuptools as the build backend
- **`[project]`** - Package metadata and dependencies
- **`name`** - Project name (used for `pip install`)
- **`dependencies`** - Required packages, installed automatically
- **`[tool.setuptools.packages.find]`** - Tells setuptools to find packages in `src/`

For CLI packages, add a `[project.scripts]` section:

```toml
[project.scripts]
my-dh-query = "my_dh_cli.cli:app"
```

This creates a command line entry point that calls the `app` function from [`my_dh_cli.cli`](https://github.com/deephaven-examples/deephaven-python-packaging/blob/main/my_dh_cli/src/my_dh_cli/cli.py) (see the CLI package's [`pyproject.toml`](https://github.com/deephaven-examples/deephaven-python-packaging/blob/main/my_dh_cli/pyproject.toml)). A package can define any number of commands in this section; the toolkit's [`pyproject.toml`](https://github.com/deephaven-examples/deephaven-python-packaging/blob/main/my_dh_toolkit/pyproject.toml) defines two.

## Manage dependencies

Dependencies are specified in the `dependencies` field:

```toml
[project]
dependencies = [
  "deephaven-server>=0.35.0",
  "click>=8.0.0",
  "pandas>=2.0.0",
]
```

Declaring `deephaven-server` is sufficient for Deephaven: it depends on a matching version of `deephaven-core`, which provides the `deephaven` module that packages import.

### Version constraints

Use version specifiers to control which versions are acceptable:

- `>=0.35.0` - Minimum version (0.35.0 or higher)
- `>=2.0.0,<3.0.0` - Version range (2.x only)
- `~=1.24.0` - Compatible release (>=1.24.0, <1.25.0)
- `==1.0.0` - Exact version (useful for fully pinned applications, but usually too strict for libraries)

Lower-bound constraints such as `deephaven-server>=0.35.0` are compatibility constraints, not reproducible locks. They say which versions the package supports. If you also need repeatable installs over time, add a separate lock file or other pinning step for the environment that installs your package.

### Optional dependencies

Define optional feature sets that users can install separately:

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

Users can install optional dependencies:

```bash
pip install my_dh_library[visualization]
pip install my_dh_library[visualization,dev]
```

## Install and distribute

Install from source in editable mode for development. Editable installs pick up source edits without reinstalling:

```bash
cd my_dh_library
pip install -e .
```

Or install normally:

```bash
pip install .
```

After installation, use the package as shown in [Packaging scenarios](#packaging-scenarios): import a library's functions after starting a server, or run a CLI package's commands directly.

To distribute a package to other machines or publish it to a package index, build a wheel:

```bash
cd my_dh_library
pip install build
python -m build
```

This creates a `.whl` file in `dist/` that can be:

- Installed locally: `pip install dist/my_dh_library-0.1.0-py3-none-any.whl`
- Distributed to others
- Published to PyPI: `python -m twine upload dist/*`

## Best practices

### Package structure

- Prefer the src-layout for packages like these examples
- Keep package names lowercase with underscores
- Match the package directory name to the import name
- Include `__init__.py` in all package directories
- In packages that define entry-point commands, keep every module imported before server startup free of Deephaven-dependent imports, and import `deephaven` lazily inside the command path that runs after startup

### Dependencies

- Specify minimum versions for Deephaven and critical dependencies
- Use version ranges for flexibility
- Group related optional dependencies
- Document any system-level dependencies, such as the Java version

### Documentation

- Include a README.md with installation and usage instructions
- Document all public functions and classes
- Explain server initialization requirements
- Include sample data so users can try the package

## Adapt an example for your own project

The repository examples are templates, meant to be copied and renamed. A practical workflow is:

1. Choose the closest example:
   - [`my_dh_library/`](https://github.com/deephaven-examples/deephaven-python-packaging/tree/main/my_dh_library) for an importable library
   - [`my_dh_cli/`](https://github.com/deephaven-examples/deephaven-python-packaging/tree/main/my_dh_cli) for one installed command
   - [`my_dh_toolkit/`](https://github.com/deephaven-examples/deephaven-python-packaging/tree/main/my_dh_toolkit) for a package that exposes both library code and commands
2. Copy that directory and rename the package under `src/` to your import name.
3. Update the copied example's `pyproject.toml`: set `name`, `version`, `description`, dependencies, and any `[project.scripts]` entries to your own values.
4. Replace the example business logic in the package modules with your own code.
5. Reinstall the package and run the same kind of smoke test shown in the example README.

The repository README has a concise adaptation checklist in [Adapt an example for your own project](https://github.com/deephaven-examples/deephaven-python-packaging#adapt-an-example-for-your-own-project), and each example directory shows the exact file layout to copy.

## Next steps

The [deephaven-python-packaging](https://github.com/deephaven-examples/deephaven-python-packaging) repository provides complete, working examples of all three packaging scenarios, along with sample data in its [`data/`](https://github.com/deephaven-examples/deephaven-python-packaging/tree/main/data) directory. Clone it and adapt the example that matches your project.

## Related documentation

- [Install and use Python packages](https://deephaven.io/core/docs/how-to-guides/install-and-use-python-packages/)
- [Use the Deephaven Python package](https://deephaven.io/core/docs/how-to-guides/deephaven-python-package/)
- [Writing your `pyproject.toml`](https://packaging.python.org/en/latest/guides/writing-pyproject-toml/)
- [src layout vs flat layout](https://packaging.python.org/en/latest/discussions/src-layout-vs-flat-layout/)
- [Creating and packaging command-line tools](https://packaging.python.org/en/latest/guides/creating-command-line-tools/)
- [Setuptools documentation](https://setuptools.pypa.io/)
- [Click documentation](https://click.palletsprojects.com/)
