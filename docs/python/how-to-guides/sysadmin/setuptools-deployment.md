---
title: Deploy Python scripts with setuptools
sidebar_label: Setuptools deployment
---

This guide shows how to create and deploy Python command-line scripts that use Deephaven. By combining pip-installable Deephaven packages with modern Python packaging tools, you can create distributable Python packages containing scripts that get installed as command-line programs available to users.

This guide follows the official [Python Packaging User Guide](https://packaging.python.org/en/latest/guides/creating-command-line-tools/) recommendations for creating command-line tools.

## Overview

Modern Python packaging using `pyproject.toml` provides a standardized way to create installable packages with command-line scripts. When combined with Deephaven's pip-installable packages, this allows you to:

- Create Python packages that use Deephaven for data processing.
- Package scripts as executable command-line programs.
- Distribute your Deephaven-based tools as wheel (`.whl`) archives.
- Install these tools system-wide or in virtual environments using `pip`.

## Prerequisites

Before creating a deployable Python package with Deephaven:

- Python 3.8 or later installed on your system.
- Basic familiarity with Python packaging and setuptools.
- Deephaven installed via pip (or included as a dependency in your package).

## Install Deephaven with pip

Deephaven can be installed using pip, making it available for use in standalone Python scripts:

```shell
pip install deephaven-server
```

For more information on Deephaven's Python packages, see [Install and use Python packages](../install-and-use-python-packages.md).

## Package structure

Modern Python packages use the **src-layout**, which is the recommended structure. A typical package for a Deephaven-based command-line tool looks like this:

```
my_dh_tool/
├── src/
│   └── my_dh_tool/
│       ├── __init__.py
│       ├── __main__.py
│       └── cli.py
├── pyproject.toml
└── README.md
```

### Directory breakdown

- **`my_dh_tool/`**: The root project directory.
- **`src/`**: Source directory (recommended src-layout).
- **`src/my_dh_tool/`**: The Python package containing your code.
- **`__init__.py`**: Makes the directory a Python package.
- **`__main__.py`**: Enables running as `python -m my_dh_tool`.
- **`cli.py`**: Contains the entry point function for your command-line script.
- **`pyproject.toml`**: Defines package metadata, dependencies, and scripts (replaces setup.py).

## Create the command-line script

Create your main script file that will serve as the entry point for your command-line tool. Following best practices, create two files:

### cli.py

This file contains the main application logic:

```python syntax
from deephaven import new_table
from deephaven.column import string_col, int_col
import argparse


def process_data(input_file, verbose=False):
    """Process data using Deephaven tables."""
    if verbose:
        print(f"Processing {input_file}...")

    # Example: Create and manipulate a Deephaven table
    source = new_table(
        [
            string_col("Name", ["Alice", "Bob", "Charlie"]),
            int_col("Age", [25, 30, 35]),
            int_col("Score", [85, 90, 88]),
        ]
    )

    # Perform operations
    result = source.update(formulas=["DoubleScore = Score * 2"])

    if verbose:
        print(f"Processed {result.size} rows")

    return result


def app():
    """Main entry point for the command-line tool."""
    parser = argparse.ArgumentParser(description="Process data with Deephaven")
    parser.add_argument("input_file", help="Input file to process")
    parser.add_argument(
        "--verbose", "-v", action="store_true", help="Enable verbose output"
    )

    args = parser.parse_args()
    result = process_data(args.input_file, args.verbose)
    print("Processing complete!")


if __name__ == "__main__":
    app()
```

### **main**.py

This file enables running the package as `python -m my_dh_tool`:

```python syntax
from my_dh_tool.cli import app

if __name__ == "__main__":
    app()
```

## Create pyproject.toml

The `pyproject.toml` file is the modern standard for Python package configuration. It replaces `setup.py` and defines your package metadata, dependencies, and command-line scripts.

Here's an example `pyproject.toml`:

```toml
[build-system]
requires = ["setuptools>=61.0", "wheel"]
build-backend = "setuptools.build_meta"

[project]
name = "my-dh-tool"
version = "0.1.0"
description = "A Deephaven-based command-line tool"
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

[project.scripts]
my-dh-tool = "my_dh_tool.cli:app"

[tool.setuptools.packages.find]
where = ["src"]
```

### Script configuration explained

The key configuration for command-line scripts is in the `[project.scripts]` section:

```toml
[project.scripts]
my-dh-tool = "my_dh_tool.cli:app"
```

This tells the build system to:

- Create a command-line script named `my-dh-tool`.
- When executed, call the `app()` function from the `cli` module in the `my_dh_tool` package.

The format is: `command-name = "package.module:function"`

## Build the package

Once your package structure is complete, build a wheel archive that can be distributed and installed.

First, install the build tool:

```shell
pip install build
```

Then build your package:

```shell
cd my_dh_tool
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
pip install dist/my_dh_tool-0.1.0-py3-none-any.whl
```

### Using pipx (recommended for command-line tools)

[pipx](https://pipx.pypa.io/) is the recommended way to install Python command-line tools, as it installs them in isolated environments:

```shell
pipx install .
```

Or from the wheel:

```shell
pipx install dist/my_dh_tool-0.1.0-py3-none-any.whl
```

After installation, the `my-dh-tool` command is available system-wide (or within your virtual environment).

### Run without installing

You can also run your tool directly from the source directory:

```shell
python -m my_dh_tool input_data.csv
```

Or use `pipx run` for temporary execution:

```shell
pipx run --spec . my-dh-tool input_data.csv
```

## Run the command-line tool

Once installed, you can run your tool from any directory:

```shell
my-dh-tool input_data.csv
```

The command executes the `app()` function in your `cli.py` file.

## Advanced configuration

### Multiple entry points

You can define multiple command-line scripts in one package:

```toml
[project.scripts]
my-dh-tool = "my_dh_tool.cli:app"
my-dh-process = "my_dh_tool.processor:process"
my-dh-report = "my_dh_tool.reporter:generate_report"
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
    result = process_data(input_file)

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


def load_config():
    config = configparser.ConfigParser()
    config_path = os.path.expanduser("~/.my_dh_tool/config.ini")

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
pip install my-dh-tool
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

- Verify all dependencies are listed in the `dependencies` field of pyproject.toml.
- Check that Deephaven is installed: `pip list | grep deephaven`.
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
- [Writing pyproject.toml](https://packaging.python.org/en/latest/guides/writing-pyproject-toml/)
- [Installing stand-alone command-line tools](https://packaging.python.org/en/latest/guides/installing-stand-alone-command-line-tools/)
- [pipx documentation](https://pipx.pypa.io/)
