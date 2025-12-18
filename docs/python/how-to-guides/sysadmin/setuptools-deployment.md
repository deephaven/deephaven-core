---
title: Build and distribute Python packages
sidebar_label: Python packaging
---

This guide shows how to create and deploy Python packages that use Deephaven. By combining pip-installable Deephaven packages with modern Python packaging tools, you can create distributable packages containing:

- **Command-line scripts** - Executable tools installed as system commands.
- **Importable libraries** - Reusable code modules for other Python projects.
- **Managed dependencies** - Automatic installation of required packages.

This guide follows the official [Python Packaging User Guide](https://packaging.python.org/en/latest/guides/writing-pyproject-toml/) recommendations.

## Overview

Modern Python packaging using [`pyproject.toml`](https://packaging.python.org/en/latest/guides/writing-pyproject-toml/) provides a standardized way to create installable packages. When combined with Deephaven's pip-installable packages, this allows you to:

- **Package library code** - Create reusable modules with Deephaven query functions and utilities that other projects can import.
- **Package command-line tools** - Build executable scripts that get installed as system commands.
- **Manage dependencies** - Automatically install Deephaven and other required packages when your package is installed.
- **Distribute packages** - Share your code as wheel (`.whl`) archives via PyPI or direct distribution.
- **Version control** - Specify compatible versions of dependencies to ensure reproducible installations.

## Prerequisites

Before creating a deployable Python package with Deephaven you must have:

- Python 3.8 or later installed on your system.
- Basic familiarity with Python packaging and setuptools.
- Deephaven installed via pip (or included as a dependency in your package).

## Quick Start

Here's the minimal workflow to create and install a Deephaven CLI package:

1. **Create the project structure:**

   ```shell
   mkdir -p my_dh_project/src/my_dh_package
   cd my_dh_project
   ```

2. **Create `src/my_dh_package/__init__.py`:**

   ```python syntax
   """My Deephaven package."""

   __version__ = "0.1.0"
   ```

3. **Create `src/my_dh_package/cli.py`:**

   ```python syntax
   import click


   @click.command()
   @click.argument("input_file")
   def app(input_file: str) -> None:
       """Process data with Deephaven."""
       from deephaven import read_csv

       data = read_csv(input_file)
       click.echo(f"Loaded {data.size} rows")
   ```

4. **Create `pyproject.toml`:**

   ```toml
   [build-system]
   requires = ["setuptools>=61.0", "wheel"]
   build-backend = "setuptools.build_meta"

   [project]
   name = "my-dh-cli"
   version = "0.1.0"
   dependencies = ["deephaven-server>=0.35.0", "click>=8.0.0"]

   [project.scripts]
   my-dh-cli = "my_dh_package.cli:app"

   [tool.setuptools.packages.find]
   where = ["src"]
   ```

5. **Install and run:**
   ```shell
   pip install -e .
   my-dh-cli data.csv
   ```

For detailed explanations and advanced features, continue reading the sections below.

## Choose your packaging approach

Different projects have different needs. Before diving into the details, identify which approach fits your use case:

### Library-only package

**Use case:** Package reusable code that other projects import. No command-line interface.

**What you'll create:**

- Importable modules with Deephaven query functions and utilities
- No `[project.scripts]` section in `pyproject.toml`
- No `__main__.py` file needed

**Example usage after installation:**

```python syntax
from my_dh_library.queries import filter_by_threshold
```

### CLI-only package

**Use case:** Build command-line tools for end users. Code is executed, not imported.

**What you'll create:**

- Entry point scripts defined in `[project.scripts]`
- CLI modules with Click decorators
- Optionally `__main__.py` for `python -m` execution

**Example usage after installation:**

```shell
my-dh-query input_data.csv
```

### Combined library and CLI

**Use case:** Provide both reusable library code and command-line tools.

**What you'll create:**

- Library modules for importing
- CLI modules with entry points
- Public API exported in `__init__.py`

**Example usage:**

```python syntax
# Import as a library
from my_dh_toolkit.queries import filter_by_threshold
```

```shell
# Run as a CLI tool
my-dh-query input_data.csv
```

## Package structure

Modern Python packages use the **src-layout**, which is the recommended structure. The exact structure depends on your use case:

**Example structure (CLI package with multiple commands):**

```
my_dh_project/
├── src/
│   └── my_dh_package/
│       ├── __init__.py
│       ├── __main__.py         # Optional: enables python -m execution
│       ├── cli.py              # Main CLI command
│       └── processor.py        # Batch processing command
├── pyproject.toml
└── README.md
```

This example shows a package with two command-line tools. For library-only packages, omit the CLI files. For single-command tools, you only need one CLI module.

### Directory breakdown

- **`my_dh_project/`**: The root project directory.
- **`src/`**: Source directory (recommended src-layout).
- **`src/my_dh_package/`**: The Python package containing your code. Note: This is the package name used in Python imports.
- **`__init__.py`**: Makes the directory a Python package (required).
- **`__main__.py`**: Optional file that enables `python -m my_dh_package` execution. Only needed if you want to support module execution.
- **`cli.py`**: Contains the entry point function for the main CLI command (`my_dh_cli`).
- **`processor.py`**: Contains the entry point function for the batch processing command (`my_dh_process`).
- **[`pyproject.toml`](https://packaging.python.org/en/latest/guides/writing-pyproject-toml/)**: Defines package metadata, dependencies, and scripts. The `[project.scripts]` section maps command names to Python functions.

## Create [`pyproject.toml`](https://packaging.python.org/en/latest/guides/writing-pyproject.toml/)

The [`pyproject.toml`](https://packaging.python.org/en/latest/guides/writing-pyproject-toml/) file is the modern standard for Python package configuration. It replaces [`setup.py`](https://packaging.python.org/en/latest/guides/distributing-packages-using-setuptools/#setup-py) and defines your package metadata, dependencies, and command-line scripts.

Here's an example [`pyproject.toml`](https://packaging.python.org/en/latest/guides/writing-pyproject-toml/):

```toml
[build-system]
requires = ["setuptools>=61.0", "wheel"]
build-backend = "setuptools.build_meta"

[project]
name = "my_dh_cli"
version = "0.1.0"
description = "A command-line tool for data processing with Deephaven"
readme = "README.md"
requires-python = ">=3.8"
authors = [
  { name = "Your Name", email = "your.email@example.com" },
]
classifiers = [
  "Development Status :: 3 - Alpha",
  "Intended Audience :: Developers",
  "Programming Language :: Python :: 3",
  "Programming Language :: Python :: 3.8",
  "Programming Language :: Python :: 3.9",
  "Programming Language :: Python :: 3.10",
  "Programming Language :: Python :: 3.11",
  "Programming Language :: Python :: 3.12",
]

# Specify required packages that will be automatically installed
dependencies = [
  "deephaven-server>=0.35.0",
  "click>=8.0.0",
]

# Define command-line scripts that will be installed
[project.scripts]
my-dh-query = "my_dh_package.cli:app"
my-dh-process = "my_dh_package.processor:process"

# Tell setuptools where to find packages (using src-layout)
[tool.setuptools.packages.find]
where = ["src"]
```

### Dependencies configuration

The `dependencies` field specifies packages that will be automatically installed when your package is installed:

```toml
dependencies = [
  "deephaven-server>=0.35.0",
  "click>=8.0.0",
]
```

When users install your package with `pip install my_dh_cli`, both Deephaven and Click will be automatically installed. This ensures your package has all required libraries.

#### Version specifiers

Use version specifiers to control which versions are acceptable:

- `>=0.35.0` - Minimum version (0.35.0 or higher)
- `>=2.0.0,<3.0.0` - Version range (2.x only)
- `~=1.24.0` - Compatible release (>=1.24.0, <1.25.0)
- `==1.0.0` - Exact version (not recommended for libraries)

#### Optional dependencies

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
  "mypy>=1.0.0",
]
```

Users install optional dependencies with:

```shell
pip install my_dh_package[visualization]
pip install -e ".[dev]"  # For development
```

### Script configuration

The key configuration for command-line scripts is in the `[project.scripts]` section:

```toml
[project.scripts]
my-dh-query = "my_dh_package.cli:app"
my-dh-process = "my_dh_package.processor:process"
```

This tells the build system to:

- Create a command-line script named `my-dh-query` that calls the `app()` function from `cli.py`.
- Create a command-line script named `my-dh-process` that calls the `process()` function from `processor.py`.

The format is: `command-name = "package.module:function"`

:::note
You can configure multiple scripts in the `[project.scripts]` section. Each script will be installed as a separate command-line tool. In this example, we define two commands:

- `my-dh-query` - Processes a single CSV file (from `cli.py`)
- `my-dh-process` - Batch processes multiple CSV files (from `processor.py`)

Each command name should clearly indicate its purpose, making it obvious what gets called when you run the command.
:::

## Create source modules

Create the Python modules for your package. The modules you need depend on your [packaging approach](#choose-your-packaging-approach):

- **CLI packages**: Entry point modules (`cli.py`, `processor.py`).
- **Library packages**: Reusable function modules (`queries.py`, `utils.py`).
- **Both**: Combination of the above.

### CLI modules

#### `cli.py`

A CLI module contains the entry point function for a command-line tool:

```python syntax
import click
from deephaven.table import Table


def my_dh_query(input_file: str, verbose: bool = False) -> Table:
    """Read a CSV file and perform a simple query operation on the data."""
    from deephaven import read_csv

    if verbose:
        click.echo(f"Processing {input_file}...")

    # Read CSV file into a Deephaven table
    source = read_csv(input_file)

    # Perform operations - example assumes a 'Score' column exists
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

#### `processor.py`

A second CLI module demonstrating multiple entry points:

```python syntax
import click
from pathlib import Path
from deephaven import read_csv
from deephaven.table import Table


def batch_process(directory: str, output_dir: str, verbose: bool = False) -> None:
    """Process multiple CSV files from a directory."""
    input_path = Path(directory)
    output_path = Path(output_dir)
    output_path.mkdir(exist_ok=True)

    csv_files = list(input_path.glob("*.csv"))

    if verbose:
        click.echo(f"Found {len(csv_files)} CSV files to process")

    for csv_file in csv_files:
        if verbose:
            click.echo(f"Processing {csv_file.name}...")

        # Read and process each file
        table = read_csv(str(csv_file))
        processed = table.update(formulas=["ProcessedScore = Score * 2"])

        # Save results (example - actual implementation would write output)
        if verbose:
            click.echo(f"  Processed {processed.size} rows")


@click.command()
@click.argument("directory", type=click.Path(exists=True, file_okay=False))
@click.option("--output", "-o", default="./output", help="Output directory")
@click.option("--verbose", "-v", is_flag=True, help="Enable verbose output")
def process(directory: str, output: str, verbose: bool) -> None:
    """Batch process CSV files with Deephaven."""
    batch_process(directory, output, verbose)
    click.echo("Batch processing complete!")


if __name__ == "__main__":
    process()
```

### Package files

#### `__init__.py`

Required to make the directory a Python package. Can be minimal or export a public API:

**Basic example:**

```python syntax
"""My Deephaven package for data processing."""

__version__ = "0.1.0"
```

**Exporting public API:**

If you want users to import functions directly from your package (e.g., `from my_dh_package import my_dh_query`), export them in `__init__.py`:

```python syntax
"""My Deephaven package for data processing."""

__version__ = "0.1.0"

# Export main functions for easier imports
from my_dh_package.cli import my_dh_query
from my_dh_package.processor import batch_process

# Control what gets imported with "from my_dh_package import *"
__all__ = ["my_dh_query", "batch_process"]
```

This allows users to write:

```python syntax
from my_dh_package import my_dh_query
```

Instead of:

```python syntax
from my_dh_package.cli import my_dh_query
```

**When to export:**

- **Library packages**: Export your public API for easier imports
- **CLI-only packages**: Keep it minimal (just version and docstring)
- **Mixed packages**: Export library functions but not CLI entry points

#### `__main__.py` (optional)

Enables `python -m my_dh_package` execution. Calls the same entry point as `[project.scripts]`:

```python syntax
from my_dh_package.cli import app

if __name__ == "__main__":
    app()
```

This allows users to run the package both ways:

- `my_dh_cli` (after installation via entry point)
- `python -m my_dh_package` (from source without installation)

### Library modules

Library modules contain reusable functions that other projects can import:

#### `queries.py`

```python syntax
"""Reusable Deephaven query functions."""

from deephaven.table import Table


def filter_by_threshold(table: Table, column: str, threshold: float) -> Table:
    """Filter table rows where column value exceeds threshold."""
    return table.where(f"{column} > {threshold}")


def add_computed_columns(table: Table) -> Table:
    """Add commonly used computed columns to a table."""
    return table.update(
        [
            "DoubleValue = Value * 2",
            "IsHigh = Value > 100",
        ]
    )


def summarize_by_group(table: Table, group_col: str, value_col: str) -> Table:
    """Create summary statistics grouped by a column."""
    return table.agg_by(
        [
            f"Sum = sum({value_col})",
            f"Avg = avg({value_col})",
            f"Count = count()",
        ],
        by=[group_col],
    )
```

#### Using library modules

Other projects can import and use your library:

```python syntax
from my_dh_package.queries import filter_by_threshold, add_computed_columns
from deephaven import read_csv

# Use the library functions
data = read_csv("data.csv")
filtered = filter_by_threshold(data, "Score", 75.0)
enhanced = add_computed_columns(filtered)
```

## Build the package

Once your package structure is complete, build a wheel archive that can be distributed and installed.

First, install the build tool:

```shell
pip install build
```

Then build your package:

```shell
cd my_dh_project
python -m build
```

This creates two files in the `dist/` directory:

- A source distribution (`.tar.gz`)
- A wheel distribution (`.whl`)

The wheel file can be distributed and installed on other systems.

## Install and run

### Development installation

Install in editable mode for development:

```shell
pip install -e .
```

This allows you to modify code without reinstalling. After installation, run your CLI tools:

```shell
my-dh-query input_data.csv
my-dh-process data/ --output results/
```

### Run without installing

Run directly from source using `python -m`:

```shell
python -m my_dh_package input_data.csv
```

This requires `__main__.py` in your package.

### Production installation

Install from a built wheel:

```shell
pip install dist/my_dh_package-0.1.0-py3-none-any.whl
```

## Distribute the package

### Publish to PyPI

```shell
python -m pip install twine
python -m twine upload dist/*
```

Users can then install with:

```shell
pip install my_dh_cli
```

### Distribute wheel files directly

Share the `.whl` file for direct installation:

```shell
pip install my_dh_package-0.1.0-py3-none-any.whl
```

## Troubleshooting

### Command not found after installation

If your command isn't found after installation:

- Ensure the installation completed successfully without errors, and that the build has not failed.
- Check that the installation directory is in your system's PATH.
- Try reinstalling with `pip install --force-reinstall`.

### Import errors

If you encounter import errors when running your command:

- Verify all dependencies are listed in the `dependencies` field of [`pyproject.toml`](https://packaging.python.org/en/latest/guides/writing-pyproject-toml/).
- Check that the required packages are installed: `pip list`.
- Ensure you're using a compatible Python version.

### Module not found errors

If Python can't find your modules:

- Verify your package structure matches the entry point definition.
- Ensure `__init__.py` files exist in all package directories.
- Check that package names in `[project.scripts]` match your directory structure.

## Related documentation

- [Install and use Python packages](../install-and-use-python-packages.md)
- [Use the Deephaven Python package](../deephaven-python-package.md)
- [Application Mode scripts](../application-mode-script.md)
- [Creating command-line tools](https://packaging.python.org/en/latest/guides/creating-command-line-tools/) (Python Packaging User Guide)
- [Writing `pyproject.toml`](https://packaging.python.org/en/latest/guides/writing-pyproject-toml/)
- [Installing stand-alone command-line tools](https://packaging.python.org/en/latest/guides/installing-stand-alone-command-line-tools/)
- [Setuptools documentation](https://setuptools.pypa.io/)
- [Building distributions](https://packaging.python.org/en/latest/guides/distributing-packages-using-setuptools/#packaging-your-project)
- [Uploading to PyPI](https://packaging.python.org/en/latest/guides/distributing-packages-using-setuptools/#uploading-your-project-to-pypi)
- [Entry points specification](https://packaging.python.org/en/latest/specifications/entry-points/)
- [Click documentation](https://click.palletsprojects.com/)
- [pipx documentation](https://pipx.pypa.io/)
