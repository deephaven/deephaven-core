---
title: Local development with Deephaven libraries
sidebar_label: Local development
---

This guide explains how to build Python projects that use Deephaven tables locally. This is useful for creating utilities, custom functions, or data pipelines that work with Deephaven tables outside of the Deephaven IDE.

## Set up your project

Install `deephaven-server` to use Deephaven tables in your Python project:

```bash
pip install deephaven-server
```

> **Note:** `deephaven-server` requires Java 17+ and sets up an embedded Deephaven server. Set your `JAVA_HOME` environment variable before running.

### Optional dependencies

| Use case                             | Package                                      |
| ------------------------------------ | -------------------------------------------- |
| Connect to a remote Deephaven server | `pydeephaven`                                |
| Read/write Parquet files             | `pyarrow` (included with `deephaven-server`) |
| NumPy integration                    | `numpy` (included with `deephaven-server`)   |
| Pandas integration                   | `pandas` (included with `deephaven-server`)  |

## Local unit testing

To use Deephaven table operations in unit tests, start an embedded server before importing Deephaven modules.

### pytest setup

Create a `conftest.py` file in your test directory to start the server once for all tests:

```python skip-test
# conftest.py
import pytest


@pytest.fixture(scope="session", autouse=True)
def deephaven_server():
    """Start Deephaven server before any tests run."""
    from deephaven_server import Server

    server = Server(port=10000, jvm_args=["-Xmx2g"])
    server.start()

    yield server

    # Server stops automatically when the process ends
```

> **Note:** You must start the server before importing any `deephaven` modules. The `deephaven_server` import initializes the JVM, and subsequent `deephaven` imports depend on it.

## Example tests

```python skip-test
# test_my_utils.py
from deephaven import empty_table, new_table
from deephaven.column import int_col, string_col


def test_table_creation():
    """Test that we can create a table."""
    t = empty_table(10).update("X = i", "Y = X * 2")

    assert t.size == 10
    assert "X" in [col.name for col in t.columns]
    assert "Y" in [col.name for col in t.columns]


def test_table_operations():
    """Test filtering and aggregation."""
    t = new_table(
        [
            string_col("Category", ["A", "B", "A", "B", "A"]),
            int_col("Value", [10, 20, 30, 40, 50]),
        ]
    )

    # Filter
    filtered = t.where("Category = `A`")
    assert filtered.size == 3

    # Aggregate
    summed = t.sum_by("Category")
    assert summed.size == 2
```

Run tests with:

```bash
pytest tests/ -v
```

## Testing ticking tables

When testing with [`time_table`](../reference/table-operations/create/timeTable.md) or other ticking tables, use [`await_update`](../reference/table-operations/table-listeners/await-update.md) to wait for updates:

```python skip-test
from deephaven import time_table
from deephaven.update_graph import exclusive_lock


def test_ticking_table():
    """Test a ticking table."""
    t = time_table("PT1S").update("X = ii")

    # Wait for the table to have at least 3 rows
    with exclusive_lock(t):
        while t.size < 3:
            t.await_update(1000)  # Wait up to 1 second

    assert t.size >= 3
```

## Testing with a remote server

If your code connects to a remote Deephaven server, use `pydeephaven`:

```bash
pip install pydeephaven
```

See [Python Client Quickstart](../getting-started/pyclient-quickstart.md) for full setup instructions.

```python skip-test
# test_client.py
import pytest
from pydeephaven import Session


@pytest.fixture(scope="module")
def session():
    """Connect to a running Deephaven server."""
    session = Session(host="localhost", port=10000)
    yield session
    session.close()


def test_fetch_table(session):
    """Test fetching a table from the server."""
    # Assumes 'my_table' exists on the server
    table = session.open_table("my_table")
    assert table.size > 0
```

## Project structure

A typical project structure:

```
my_project/
├── src/
│   └── my_utils.py
├── tests/
│   ├── conftest.py      # Server setup
│   ├── test_my_utils.py
│   └── data/
│       └── test_data.csv
├── pyproject.toml
└── README.md
```

### Example pyproject.toml

```toml skip-test
[project]
name = "my-deephaven-project"
version = "0.1.0"
requires-python = ">=3.9"
dependencies = [
  "deephaven-server>=0.37.0",
]

[project.optional-dependencies]
test = [
  "pytest>=7.0",
]

[tool.pytest.ini_options]
testpaths = ["tests"]
```

## Reading test data

Load test data from CSV or Parquet files:

```python skip-test
from deephaven import read_csv
from deephaven.parquet import read


def test_csv_data():
    """Test with CSV data."""
    t = read_csv("tests/data/test_data.csv")
    assert t.size > 0


def test_parquet_data():
    """Test with Parquet data."""
    t = read("tests/data/test_data.parquet")
    assert t.size > 0
```

See [`read_csv`](../reference/data-import-export/CSV/readCsv.md) and [`read`](../reference/data-import-export/Parquet/readTable.md) for more options.

## Related documentation

- [Install with pip](../getting-started/pip-install.md)
- [Python client quickstart](../getting-started/pyclient-quickstart.md)
- [Create tables](./new-and-empty-table.md)
