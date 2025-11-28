---
id: custom-data-sources
title: Can I integrate custom data sources with Deephaven?
sidebar_label: Can I integrate custom data sources?
---

Yes, you can integrate custom data sources with Deephaven. While Deephaven includes a proprietary columnar store for persistent historical and intraday data, you can integrate your own data stores to leverage Deephaven's efficient engine, analytics, and data visualization capabilities.

There are three main integration approaches:

- **Static in-memory tables** - Similar to CSV and JDBC imports.
- **Dynamic in-memory tables** - For real-time data feeds like multicast distribution systems.
- **Lazily-loaded on-disk tables** - For large datasets like Apache Parquet files.

## Understanding Deephaven table structure

Each Deephaven table consists of:

- A [RowSet](https://deephaven.io/core/javadoc/io/deephaven/engine/rowset/RowSet.html) - An ordered set of long keys representing valid row addresses.
- Named [ColumnSources](https://deephaven.io/core/javadoc/io/deephaven/engine/table/ColumnSource.html) - A map from column name to ColumnSource, which acts as a dictionary from row key to cell value.

In Python, you typically use higher-level APIs like `new_table()`, `DynamicTableWriter`, or pandas integration rather than working directly with RowSets and ColumnSources.

## Static in-memory tables

For simple static data sources, use `new_table()` to create tables from Python data structures.

Here's an example of creating a static table from custom data:

```python order=custom_table
from deephaven import new_table
from deephaven.column import string_col, double_col, int_col

# Your custom data as Python lists
symbols = ["AAPL", "GOOGL", "MSFT", "AMZN"]
prices = [150.25, 2800.50, 380.75, 3400.00]
volumes = [1000000, 500000, 750000, 600000]

# Create the table
custom_table = new_table(
    [
        string_col("Symbol", symbols),
        double_col("Price", prices),
        int_col("Volume", volumes),
    ]
)
```

The `new_table()` function automatically infers column types from the provided data.

### Alternative: Using pandas

You can also create tables from pandas DataFrames:

```python order=custom_table
import pandas as pd
from deephaven import pandas as dhpd

# Create a pandas DataFrame
df = pd.DataFrame(
    {
        "Symbol": ["AAPL", "GOOGL", "MSFT", "AMZN"],
        "Price": [150.25, 2800.50, 380.75, 3400.00],
        "Volume": [1000000, 500000, 750000, 600000],
    }
)

# Convert to Deephaven table
custom_table = dhpd.to_table(df)
```

## Dynamic in-memory tables

Dynamic tables allow you to integrate real-time data feeds. These tables update on each Deephaven update cycle and notify downstream operations of changes.

The easiest way to create dynamic tables in Python is using `DynamicTableWriter`:

```python order=dynamic_table
from deephaven import DynamicTableWriter
import deephaven.dtypes as dht

# Define the table schema
column_definitions = {"Symbol": dht.string, "Price": dht.double}

# Create a dynamic table writer
table_writer = DynamicTableWriter(column_definitions)
dynamic_table = table_writer.table

# Write initial data
for i in range(100):
    table_writer.write_row(f"SYM{i}", 100.0 + i)

# In your update logic, continue writing rows:
# table_writer.write_row("AAPL", 150.25)
```

### Using table replayer for time-based data

For replaying historical data or simulating real-time feeds:

```python order=replayed_table
from deephaven import new_table
from deephaven.column import string_col, double_col, datetime_col
from deephaven.replay import TableReplayer
from deephaven.time import to_j_instant

# Create source data with timestamps
source_data = new_table(
    [
        string_col("Symbol", ["AAPL", "GOOGL", "MSFT"]),
        double_col("Price", [150.25, 2800.50, 380.75]),
        datetime_col(
            "Timestamp",
            [
                to_j_instant("2024-01-01T09:30:00 ET"),
                to_j_instant("2024-01-01T09:30:01 ET"),
                to_j_instant("2024-01-01T09:30:02 ET"),
            ],
        ),
    ]
)

# Create a replayer that replays data based on timestamps
replayer = TableReplayer(
    to_j_instant("2024-01-01T09:30:00 ET"), to_j_instant("2024-01-01T09:35:00 ET")
)
replayed_table = replayer.add_table(source_data, "Timestamp")
replayer.start()
```

## Advanced: Java interop for custom ColumnSources

For advanced use cases requiring custom data loading logic, you can use Java interop to create custom ColumnSources. This approach is similar to the Groovy examples but uses Python's Java integration.

> [!WARNING]
> This is an advanced technique requiring knowledge of Deephaven's Java internals. For most use cases, `new_table()`, `DynamicTableWriter`, or pandas integration are recommended.

```python skip-test
from deephaven.jcompat import j_hashmap
import jpy

# Import Java classes
RowSetFactory = jpy.get_type("io.deephaven.engine.rowset.RowSetFactory")
ArrayBackedColumnSource = jpy.get_type(
    "io.deephaven.engine.table.impl.sources.ArrayBackedColumnSource"
)
QueryTable = jpy.get_type("io.deephaven.engine.table.impl.QueryTable")

# Your custom data as Java arrays
symbols = jpy.array("java.lang.String", ["AAPL", "GOOGL", "MSFT", "AMZN"])
prices = jpy.array("double", [150.25, 2800.50, 380.75, 3400.00])
volumes = jpy.array("int", [1000000, 500000, 750000, 600000])

# Create a TrackingRowSet
row_set = RowSetFactory.flat(len(symbols)).toTracking()

# Create column sources
# Note: Primitive arrays (int, double) infer type automatically,
# but object arrays (String) require explicit type specification
column_sources = j_hashmap(
    {
        "Symbol": ArrayBackedColumnSource.getMemoryColumnSource(
            symbols, jpy.get_type("java.lang.String"), None
        ),
        "Price": ArrayBackedColumnSource.getMemoryColumnSource(prices),
        "Volume": ArrayBackedColumnSource.getMemoryColumnSource(volumes),
    }
)

# Create the QueryTable
custom_table = QueryTable(row_set, column_sources)
```

## Working with external data sources

### Reading from custom file formats

For custom file formats, read the data into Python structures and use `new_table()`:

```python skip-test
import struct
from deephaven import new_table
from deephaven.column import double_col


def read_custom_binary_file(filepath):
    """Example: Read doubles from a custom binary format"""
    values = []
    with open(filepath, "rb") as f:
        while True:
            chunk = f.read(8)  # Read 8 bytes for a double
            if not chunk:
                break
            values.append(struct.unpack("d", chunk)[0])
    return values


# Read data and create table
data = read_custom_binary_file("data.bin")
custom_table = new_table([double_col("Value", data)])
```

### Streaming data from APIs

For streaming data from external APIs, use `DynamicTableWriter` with a background thread:

```python skip-test
import threading
import time
from deephaven import DynamicTableWriter
import deephaven.dtypes as dht

# Create dynamic table
column_definitions = {
    "Timestamp": dht.Instant,
    "Symbol": dht.string,
    "Price": dht.double,
}

table_writer = DynamicTableWriter(column_definitions)
streaming_table = table_writer.table


def fetch_and_write_data():
    """Background thread that fetches data from an API"""
    while True:
        # Fetch data from your API
        # data = fetch_from_api()

        # Write to table
        # table_writer.write_row(data['timestamp'], data['symbol'], data['price'])

        time.sleep(1)  # Poll interval


# Start background thread
thread = threading.Thread(target=fetch_and_write_data, daemon=True)
thread.start()
```

## Related documentation

- [Parquet](../../how-to-guides/data-import-export/parquet-export.md)
- [Iceberg](../../how-to-guides/data-import-export/iceberg.md)
- [DynamicTableWriter](../table-operations/create/DynamicTableWriter.md)
- [new_table](../table-operations/create/newTable.md)
- [Deephaven Core Javadoc](https://deephaven.io/core/javadoc/)

> [!NOTE]
> These FAQ pages contain answers to questions about Deephaven Community Core that our users have asked in our [Community Slack](/slack). If you have a question that is not in our documentation, [join our Community](/slack) and we'll be happy to help!
