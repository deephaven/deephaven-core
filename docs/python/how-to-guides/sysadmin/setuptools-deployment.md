---
title: Deploy Python scripts with setuptools
sidebar_label: Setuptools deployment
---

This guide shows how to create and deploy Python command-line scripts that use Deephaven. By combining pip-installable Deephaven packages with modern Python packaging tools, you can create distributable Python packages containing scripts that get installed as command-line programs available to users.

This guide follows the official [Python Packaging User Guide](https://packaging.python.org/en/latest/guides/creating-command-line-tools/) recommendations for creating command-line tools.

## Overview

Modern Python packaging using [`pyproject.toml`](https://packaging.python.org/en/latest/guides/writing-pyproject-toml/) provides a standardized way to create installable packages with command-line scripts. When combined with Deephaven's pip-installable packages, this allows you to:

- Create Python packages that use Deephaven for data processing.
- Package scripts as executable command-line programs.
- Distribute your Deephaven-based tools as wheel (`.whl`) archives.
- Install these tools system-wide or in virtual environments using `pip`.

## Prerequisites

Before creating a deployable Python package with Deephaven you must have:

- Python 3.8 or later installed on your system.
- Basic familiarity with Python packaging and setuptools.
- Deephaven installed via pip (or included as a dependency in your package).

## Package structure

Modern Python packages use the **src-layout**, which is the recommended structure. A typical package for a Deephaven-based command-line tool looks like this:

```
my_dh_project/
├── src/
│   └── my_dh_package/
│       ├── __init__.py
│       ├── __main__.py
│       └── cli.py
├── pyproject.toml
└── README.md
```

### Directory breakdown

- **`my_dh_project/`**: The root project directory.
- **`src/`**: Source directory (recommended src-layout).
- **`src/my_dh_package/`**: The Python package containing your code. Note: This is the package name used in Python imports.
- **`__init__.py`**: Makes the directory a Python package.
- **`__main__.py`**: Special file that allows the package to be executed as a module with `python -m my_dh_package` (where `my_dh_package` is the package name).
- **`cli.py`**: Contains the entry point function for your command-line script.
- **[`pyproject.toml`](https://packaging.python.org/en/latest/guides/writing-pyproject-toml/)**: Defines package metadata, dependencies, and scripts. The `[project.scripts]` section maps command names (e.g., `my_dh_cli`) to Python functions, creating executable commands after installation.

## Create the command-line script

Create your main script files that will serve as the entry point for your command-line tool. Following best practices, create three files:

### `cli.py`

This file is a python module that contains the main application logic - a library function and a function that is the entry point for a cli script:

```python syntax
import argparse
from deephaven.table import Table


def my_dh_query(input_file: str, verbose: bool = False) -> Table:
    """Read a CSV file and perform a simple query operation on the data."""
    from deephaven import read_csv
    
    if verbose:
        print(f"Processing {input_file}...")

    # Read CSV file into a Deephaven table
    source = read_csv(input_file)

    # Perform operations - example assumes a 'Score' column exists
    result = source.update(formulas=["DoubleScore = Score * 2"])

    if verbose:
        print(f"Processed {result.size} rows")

    return result


def app() -> None:
    """Main entry point for the command-line tool."""
    parser = argparse.ArgumentParser(description="Process data with Deephaven")
    parser.add_argument("input_file", help="Input file to process")
    parser.add_argument(
        "--verbose", "-v", action="store_true", help="Enable verbose output"
    )

    args = parser.parse_args()
    result = my_dh_query(args.input_file, args.verbose)
    print("Processing complete!")


if __name__ == "__main__":
    app()
```

### `__init__.py`

This file makes the directory a Python package. For a simple command-line tool, it can be empty or contain package-level initialization:

```python syntax
"""My Deephaven package for data processing."""

__version__ = "0.1.0"
```

### `__main__.py`

This file enables running the package as `python -m my_dh_package`:

```python syntax
from my_dh_package.cli import app

if __name__ == "__main__":
    app()
```

## Create [`pyproject.toml`](https://packaging.python.org/en/latest/guides/writing-pyproject-toml/)

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
dependencies = [
  "deephaven-server>=0.35.0",
]

# Define command-line scripts that will be installed
[project.scripts]
my_dh_cli = "my_dh_package.cli:app"

# Tell setuptools where to find packages (using src-layout)
[tool.setuptools.packages.find]
where = ["src"]
```

### Script configuration explained

The key configuration for command-line scripts is in the `[project.scripts]` section:

```toml
[project.scripts]
my_dh_cli = "my_dh_package.cli:app"
```

This tells the build system to:

- Create a command-line script named `my_dh_cli`.
- When executed, call the `app()` function from the `cli` module in the `my_dh_package` package.

The format is: `command-name = "package.module:function"`

:::note
You can configure multiple scripts in the `[project.scripts]` section. Each script will be installed as a separate command-line tool. For example:

```toml
[project.scripts]
my_dh_cli = "my_dh_package.cli:app"
my_dh_process = "my_dh_package.processor:process"
my_dh_report = "my_dh_package.reporter:generate_report"
```
:::

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

## Install the package

### For development

Install your package in editable mode for development:

```shell
pip install -e .
```

This allows you to make changes to your code without reinstalling.

### For regular use

Install from the built wheel:

```shell
pip install dist/my_dh_package-0.1.0-py3-none-any.whl
```

### Using pipx (recommended for command-line tools)

[pipx](https://pipx.pypa.io/) is the recommended way to install Python command-line tools, as it installs them in isolated environments:

```shell
pipx install .
```

Or from the wheel:

```shell
pipx install dist/my_dh_package-0.1.0-py3-none-any.whl
```

After installation, the `my_dh_cli` command is available system-wide (or within your virtual environment).

### Run without installing

You can run your package directly from the source directory using `python -m` with the package name (not the command name):

```shell
python -m my_dh_package input_data.csv
```

This executes the `__main__.py` file, which calls the `app()` function.

## Run the command-line tool

Once installed, the command defined in `[project.scripts]` becomes available. You can run it from any directory using the command name:

```shell
my_dh_cli input_data.csv
```

Note: `my_dh_cli` is the command name (defined in `pyproject.toml`), while `my_dh_package` is the package name (the directory under `src/`). The command executes the `app()` function in your `cli.py` file.

## Advanced configuration

### Multiple entry points

You can define multiple command-line scripts in one package:

```toml
[project.scripts]
my_dh_cli = "my_dh_package.cli:app"
my_dh_process = "my_dh_package.processor:process"
my_dh_report = "my_dh_package.reporter:generate_report"
```

### Advanced argument parsing

For more sophisticated CLIs, consider using [typer](https://typer.tiangolo.com/) or [click](https://click.palletsprojects.com/):

```python syntax
import typer
from typing_extensions import Annotated


def app(
    input_file: Annotated[str, typer.Argument(help="Input file to process")],
    output: Annotated[str, typer.Option(help="Output file")] = None,
    verbose: Annotated[bool, typer.Option(help="Verbose output")] = False,
):
    """Process data with Deephaven."""
    if verbose:
        typer.echo(f"Processing {input_file}...")

    # Process data using Deephaven
    result = my_dh_query(input_file)

    if output:
        typer.echo(f"Results saved to {output}")


if __name__ == "__main__":
    typer.run(app)
```

Add `typer` to your dependencies:

```toml
dependencies = [
  "deephaven-server>=0.35.0",
  "typer>=0.9.0",
]
```

### Configuration files

For more complex tools, consider using configuration files:

```python syntax
import configparser
import os


def load_config() -> configparser.ConfigParser:
    config = configparser.ConfigParser()
    config_path = os.path.expanduser("~/.my_dh_package/config.ini")

    if os.path.exists(config_path):
        config.read(config_path)

    return config
```

## Distribution

### Distribute via PyPI

Once your package is ready, you can publish it to the Python Package Index (PyPI):

```shell
python -m pip install twine
python -m twine upload dist/*
```

Users can then install your tool with:

```shell
pip install my_dh_cli
```

### Distribute as wheel files

Alternatively, distribute the `.whl` file directly to users who can install it with:

```shell
pip install my_dh_tool-0.1.0-py3-none-any.whl
```

## Best practices

- **Version pinning**: Specify minimum Deephaven versions in the `dependencies` field to ensure compatibility.
- **Error handling**: Add robust error handling for file operations and data processing.
- **Use src-layout**: Follow the recommended src-layout structure for better package organization.
- **Logging**: Use Python's `logging` module instead of `print()` for production tools.
- **Testing**: Include unit tests for your command-line scripts.
- **Documentation**: Provide clear usage instructions in your README and command-line help text.
- **Virtual environments**: Develop and test in isolated virtual environments to avoid dependency conflicts.

## Troubleshooting

### Command not found after installation

If your command isn't found after installation:

- Ensure the installation completed successfully without errors.
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
- [pipx documentation](https://pipx.pypa.io/)
