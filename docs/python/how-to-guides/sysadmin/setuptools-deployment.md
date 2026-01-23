---
title: Packaging custom code and dependencies
sidebar_label: Python packaging
---

[Python packaging](https://packaging.python.org/en/latest/guides/writing-pyproject-toml/) enables you to create distributable packages containing custom code, command-line tools, and managed dependencies. Deephaven's pip-installable packages integrate seamlessly with modern Python packaging tools, allowing you to build reusable libraries and executable scripts that leverage Deephaven's query engine. This guide walks through the concepts and patterns for packaging Deephaven-based Python projects.

Python packaging with [`pyproject.toml`](https://packaging.python.org/en/latest/guides/writing-pyproject-toml/) provides:

- **Reusable libraries** - Package query functions and utilities for import by other projects.
- **Command-line tools** - Build executable scripts with entry point definitions.
- **Dependency management** - Automatically install Deephaven and required packages.
- **Distribution** - Share code as wheel archives via PyPI or direct distribution.
- **Version control** - Specify compatible dependency versions for reproducible installations.

## Example repository

The examples in this guide use the [deephaven-python-packaging](https://github.com/deephaven-examples/deephaven-python-packaging) repository. It demonstrates three complete packaging scenarios with working code, sample data, and comprehensive documentation.

To explore the examples, clone the repository:

```bash
git clone https://github.com/deephaven-examples/deephaven-python-packaging.git
cd deephaven-python-packaging
```

The repository contains three example packages:

- `my_dh_library/` - Library-only package with reusable query functions.
- `my_dh_cli/` - CLI-only package with command-line tools.
- `my_dh_toolkit/` - Combined package with both library and CLI functionality.

## Package structure

Modern Python packages use the **src-layout**, which is the recommended structure by the [Python Packaging Authority](https://packaging.python.org/en/latest/guides/writing-pyproject-toml/). This layout keeps source code separate from tests and configuration files:

```
my_dh_project/
├── src/
│   └── my_dh_package/
│       ├── __init__.py
│       ├── queries.py
│       └── utils.py
├── pyproject.toml
└── README.md
```

### Key components

- **`src/`** - Source directory containing the package code.
- **`my_dh_package/`** - The Python package (directory name used in imports).
- **`__init__.py`** - Makes the directory importable and can export public API.
- **`pyproject.toml`** - Defines package metadata, dependencies, and entry points.
- **Module files** - Python files containing your functions and classes.

The package name under `src/` determines how users import your code. For example, with `src/my_dh_library/`, users import via `from my_dh_library import ...`.

## Packaging scenarios

Different projects have different needs. The example repository demonstrates three common scenarios:

### Library-only package

Package reusable code without CLI tools. Other projects import your modules.

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

data = read_csv("data.csv")
filtered = filter_by_threshold(data, "Score", 75.0)
```

**When to use:**

- Creating reusable utilities for other projects.
- No command-line interface needed.
- Code will be imported, not executed directly.

### CLI-only package

Package executable command-line tools without exposing library code.

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

```python
# CLI functions are used within a Python session
from my_dh_cli.cli import my_dh_query
result = my_dh_query("data.csv", verbose=True)
```

**When to use:**

- Building command-line tools for data processing
- Want clean function interfaces
- No library code to expose to other projects

### Combined package

Package both reusable library code and command-line tools.

**Structure:**

```
my_dh_toolkit/
├── src/
│   └── my_dh_toolkit/
│       ├── __init__.py
│       ├── __main__.py
│       ├── cli.py
│       ├── processor.py
│       ├── queries.py
│       └── utils.py
├── pyproject.toml
└── README.md
```

**Usage:**

```python
# As a library
from my_dh_toolkit.queries import filter_by_threshold
from my_dh_toolkit import my_dh_query

# Use library functions
data = read_csv("data.csv")
filtered = filter_by_threshold(data, "Score", 75.0)

# Or use CLI functions
result = my_dh_query("data.csv", verbose=True)
```

**When to use:**

- Need both library and CLI functionality
- Want to provide multiple interfaces to the same code
- Library functions are useful independently

## Create a new package

<details>
<summary>Step-by-step instructions for creating packages from scratch</summary>

This section walks through creating each type of package from scratch.

### Create a library-only package

Create the directory structure:

```bash
mkdir -p my_dh_library/src/my_dh_library
cd my_dh_library
```

Create `pyproject.toml`:

```toml
[build-system]
requires = ["setuptools>=61.0", "wheel"]
build-backend = "setuptools.build_meta"

[project]
name = "my_dh_library"
version = "0.1.0"
description = "Reusable Deephaven query functions"
readme = "README.md"
requires-python = ">=3.8"
dependencies = [
  "deephaven-server>=0.35.0",
]

[tool.setuptools.packages.find]
where = ["src"]
```

Create `src/my_dh_library/__init__.py`:

```python
"""My Deephaven library for data processing."""

__version__ = "0.1.0"

from my_dh_library.queries import filter_by_threshold, add_computed_columns, summarize_by_group

__all__ = ["filter_by_threshold", "add_computed_columns", "summarize_by_group"]
```

Create `src/my_dh_library/queries.py`:

```python
"""Reusable Deephaven query functions."""

from deephaven.table import Table


def filter_by_threshold(table: Table, column: str, threshold: float) -> Table:
    """Filter table rows where column value exceeds threshold."""
    return table.where(f"{column} > {threshold}")


def add_computed_columns(table: Table) -> Table:
    """Add commonly used computed columns to a table."""
    return table.update([
        "DoubleValue = Value * 2",
        "IsHigh = Value > 100",
    ])


def summarize_by_group(table: Table, group_col: str, value_col: str) -> Table:
    """Create summary statistics grouped by a column."""
    return table.agg_by([
        f"Sum = sum({value_col})",
        f"Avg = avg({value_col})",
        f"Count = count()",
    ], by=[group_col])
```

Create `src/my_dh_library/utils.py`:

```python
"""Utility functions for working with Deephaven tables."""

from deephaven.table import Table


def validate_columns(table: Table, required_columns: list[str]) -> bool:
    """Check if table has all required columns."""
    table_columns = [col.name for col in table.columns]
    return all(col in table_columns for col in required_columns)


def get_table_info(table: Table) -> dict:
    """Get basic information about a table."""
    return {
        "num_rows": table.size,
        "num_columns": len(table.columns),
        "columns": [col.name for col in table.columns],
    }
```

Create `README.md` with installation and usage instructions.

### Create a CLI-only package

Create the directory structure:

```bash
mkdir -p my_dh_cli/src/my_dh_cli
cd my_dh_cli
```

Create `pyproject.toml`:

```toml
[build-system]
requires = ["setuptools>=61.0", "wheel"]
build-backend = "setuptools.build_meta"

[project]
name = "my_dh_cli"
version = "0.1.0"
description = "Command-line tool for data processing"
readme = "README.md"
requires-python = ">=3.8"
dependencies = [
  "deephaven-server>=0.35.0",
  "click>=8.0.0",
]

[project.scripts]
my-dh-query = "my_dh_cli.cli:app"

[tool.setuptools.packages.find]
where = ["src"]
```

Create `src/my_dh_cli/__init__.py`:

```python
"""My Deephaven CLI package."""

__version__ = "0.1.0"
```

Create `src/my_dh_cli/__main__.py`:

```python
from my_dh_cli.cli import app

if __name__ == "__main__":
    app()
```

Create `src/my_dh_cli/cli.py`:

```python
import click


def my_dh_query(input_file: str, verbose: bool = False):
    """Read a CSV file and perform a simple query operation on the data."""
    from deephaven import read_csv

    if verbose:
        click.echo(f"Processing {input_file}...")

    source = read_csv(input_file)

    result = source.update(formulas=["DoubleScore = Score * 2"])

    if verbose:
        click.echo(f"Processed {result.size} rows")

    return result


@click.command()
@click.argument("input_file", type=click.Path(exists=True))
@click.option("--verbose", "-v", is_flag=True, help="Enable verbose output")
def app(input_file: str, verbose: bool) -> None:
    """Process data with Deephaven."""
    result = my_dh_query(input_file, verbose)
    click.echo("Processing complete!")


if __name__ == "__main__":
    app()
```

Create `README.md` with installation and usage instructions.

### Create a combined package

Create the directory structure:

```bash
mkdir -p my_dh_toolkit/src/my_dh_toolkit
cd my_dh_toolkit
```

Create `pyproject.toml`:

```toml
[build-system]
requires = ["setuptools>=61.0", "wheel"]
build-backend = "setuptools.build_meta"

[project]
name = "my_dh_toolkit"
version = "0.1.0"
description = "Deephaven library and CLI tools"
readme = "README.md"
requires-python = ">=3.8"
dependencies = [
  "deephaven-server>=0.35.0",
  "click>=8.0.0",
]

[project.scripts]
my-dh-query = "my_dh_toolkit.cli:app"
my-dh-process = "my_dh_toolkit.processor:process"

[tool.setuptools.packages.find]
where = ["src"]
```

Create `src/my_dh_toolkit/__init__.py`:

```python
"""My Deephaven toolkit for data processing."""

__version__ = "0.1.0"

from my_dh_toolkit.cli import my_dh_query
from my_dh_toolkit.processor import batch_process

__all__ = ["my_dh_query", "batch_process"]
```

Create `src/my_dh_toolkit/__main__.py`:

```python
from my_dh_toolkit.cli import app

if __name__ == "__main__":
    app()
```

Create the library modules (`queries.py`, `utils.py`) using the same code as the library-only package.

Create `src/my_dh_toolkit/cli.py` using the same code as the CLI-only package.

Create `src/my_dh_toolkit/processor.py`:

```python
import click
from pathlib import Path


def batch_process(directory: str, output_dir: str, verbose: bool = False) -> None:
    """Process multiple CSV files from a directory."""
    from deephaven import read_csv
    
    input_path = Path(directory)
    output_path = Path(output_dir)
    output_path.mkdir(exist_ok=True)

    csv_files = list(input_path.glob("*.csv"))

    if verbose:
        click.echo(f"Found {len(csv_files)} CSV files to process")

    for csv_file in csv_files:
        if verbose:
            click.echo(f"Processing {csv_file.name}...")

        table = read_csv(str(csv_file))
        processed = table.update(formulas=["DoubleScore = Score * 2"])

        if verbose:
            click.echo(f"  Processed {processed.size} rows")


@click.command()
@click.argument("directory", type=click.Path(exists=True))
@click.option("--output", "-o", default="./output", help="Output directory")
@click.option("--verbose", "-v", is_flag=True, help="Enable verbose output")
def process(directory: str, output: str, verbose: bool) -> None:
    """Batch process CSV files from a directory."""
    batch_process(directory, output, verbose)
    click.echo("Batch processing complete!")


if __name__ == "__main__":
    process()
```

Create `README.md` with installation and usage instructions.

</details>

## Configure `pyproject.toml`

The [`pyproject.toml`](https://packaging.python.org/en/latest/guides/writing-pyproject-toml/) file defines your package configuration.

### Configuration options

Here's a detailed breakdown of `pyproject.toml` for a library-only package:

```toml
[build-system]
requires = ["setuptools>=61.0", "wheel"]
build-backend = "setuptools.build_meta"

[project]
name = "my_dh_library"
version = "0.1.0"
description = "Reusable Deephaven query functions"
readme = "README.md"
requires-python = ">=3.8"
dependencies = [
  "deephaven-server>=0.35.0",
]

[tool.setuptools.packages.find]
where = ["src"]
```

### Key sections

- **`[build-system]`** - Specifies setuptools as the build backend
- **`[project]`** - Package metadata and dependencies
- **`name`** - Project name (used for `pip install`)
- **`dependencies`** - Required packages installed automatically
- **`[tool.setuptools.packages.find]`** - Tells setuptools to find packages in `src/`

For CLI packages, add a `[project.scripts]` section:

```toml
[project.scripts]
my-dh-query = "my_dh_cli.cli:app"
```

This creates a command-line entry point that calls the `app` function from `my_dh_cli.cli`.

## Managing dependencies

Dependencies are specified in the `dependencies` field:

```toml
[project]
dependencies = [
  "deephaven-server>=0.35.0",
  "click>=8.0.0",
  "pandas>=2.0.0",
]
```

### Version constraints

Use version specifiers to control which versions are acceptable:

- `>=0.35.0` - Minimum version (0.35.0 or higher)
- `>=2.0.0,<3.0.0` - Version range (2.x only)
- `~=1.24.0` - Compatible release (>=1.24.0, <1.25.0)
- `==1.0.0` - Exact version (not recommended for libraries)

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

## Installation and usage

### Install a package

Install from source in editable mode for development:

```bash
cd my_dh_library
pip install -e .
```

Or install normally:

```bash
pip install .
```

### Use a library package

After installation, import and use the library functions:

```python
# Start the Deephaven server
from deephaven_server import Server
server = Server(port=10000, jvm_args=["-Xmx4g"])
server.start()

# Import and use library functions
from my_dh_library.queries import filter_by_threshold
from deephaven import read_csv

data = read_csv("data.csv")
filtered = filter_by_threshold(data, "Score", 75.0)
```

> [!NOTE]
> All Deephaven functionality requires a running server. Start the server before importing Deephaven modules.

### Use CLI functions

CLI functions must be used within the same Python session as the server:

```python
# Start the Deephaven server
from deephaven_server import Server
server = Server(port=10000, jvm_args=["-Xmx4g"])
server.start()

# Use CLI functions
from my_dh_cli.cli import my_dh_query
result = my_dh_query("data.csv", verbose=True)
```

## Building and distributing

Build a distributable wheel:

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

- Use the src-layout for all packages
- Keep package names lowercase with underscores
- Match the package directory name to the import name
- Include `__init__.py` in all package directories

### Dependencies

- Specify minimum versions for Deephaven and critical dependencies
- Use version ranges for flexibility
- Group related optional dependencies
- Document any system-level dependencies

### Documentation

- Include a comprehensive README.md
- Document all public functions and classes
- Provide usage examples
- Explain server initialization requirements

### Testing

- Write tests for all public functions
- Test with different Deephaven versions
- Include sample data for testing
- Document how to run tests

## Server initialization

Deephaven requires a running server before using any Deephaven functionality. The server must be initialized in the same Python process that uses Deephaven:

```python
from deephaven_server import Server

# Initialize and start the server
server = Server(port=10000, jvm_args=["-Xmx4g"])
server.start()

# Now you can import and use Deephaven
from deephaven import read_csv
data = read_csv("data.csv")
```

### Key points

- Each Python process has its own JVM
- Starting a server in one terminal doesn't help another terminal
- CLI tools must run in the same session as the server
- The server uses approximately 4GB of memory by default (configurable via `jvm_args`)

## Next steps

The [deephaven-python-packaging](https://github.com/deephaven-examples/deephaven-python-packaging) repository provides complete, working examples of all three packaging scenarios. Clone the repository and explore the examples to see how to structure your own Deephaven packages.

Each example includes:

- Complete source code
- Configured `pyproject.toml`
- Sample data files
- Comprehensive README
- Usage examples

## Related documentation

- [Install and use Python packages](https://deephaven.io/core/docs/how-to-guides/install-and-use-python-packages/)
- [Use the Deephaven Python package](https://deephaven.io/core/docs/how-to-guides/deephaven-python-package/)
- [Python Packaging User Guide](https://packaging.python.org/en/latest/guides/writing-pyproject-toml/)
- [Creating command-line tools](https://packaging.python.org/en/latest/guides/creating-command-line-tools/)
- [Setuptools documentation](https://setuptools.pypa.io/)
- [Click documentation](https://click.palletsprojects.com/)
