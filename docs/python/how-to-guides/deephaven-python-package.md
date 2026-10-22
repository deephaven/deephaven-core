---
title: Deephaven's Python package
---

The Deephaven [Python package](https://pypi.org/project/deephaven-core/) (`deephaven`) is the server-side Python API for writing queries that run inside the Deephaven engine. Queries you write with it work the same way on [static tables](../conceptual/table-types.md) and on real-time, [ticking tables](../conceptual/table-types.md), and the results update as new data arrives.

To connect to a Deephaven server from a separate Python process, use the [Python client](../getting-started/pyclient-quickstart.md) (`pydeephaven`) instead.

Deephaven's [Python API](/core/pydoc/) provides functions for creating, ingesting, transforming, and exporting tables. This guide gives a brief overview in two parts. The first part shows common tasks with short examples. The second part lists each submodule in the package, with a link to its Pydoc and related guides.

## What can you do with `deephaven`?

In short: a lot. Let's look at a few common tasks that only scratch the surface.

### Create tables from scratch

You can create tables from scratch in several ways. The code below uses three of them:

- [`empty_table`](../reference/table-operations/create/emptyTable.md) creates a table with the number of rows you ask for. [`update`](../reference/table-operations/select/update.md) then adds columns to it.
- [`new_table`](../reference/table-operations/create/newTable.md) builds a table from columns you define with column functions such as [`int_col`](../reference/table-operations/create/intCol.md), from the [`column`](#column) module.
- [`time_table`](../reference/table-operations/create/timeTable.md) creates a [ticking table](../conceptual/table-types.md), which is a table whose rows change as new data arrives. This one adds a row every second.

The [`table_factory`](#table_factory) module defines all three functions. The top-level `deephaven` package re-exports them, so the example imports them from `deephaven`.

```python order=t1_static,t2_static,t_ticking ticking-table
from deephaven.column import int_col, double_col, string_col
from deephaven import empty_table, new_table, time_table

t1_static = empty_table(50).update(["X = 0.1 * i", "Y = sin(X)", "Z = cos(X)"])
t2_static = new_table(
    [
        string_col(
            "Strings",
            ["A", "Hello world!", "The quick brown fox jumps over the lazy dog."],
        ),
        int_col("Integers", [5, 17, -33422]),
        double_col("Doubles", [3.14, -1.1111, 99.999]),
    ]
)
t_ticking = time_table("PT1s").update("X = ii")
```

### Ingest data from external sources

Ingest data into tables from external sources such as [CSV](./data-import-export/csv-import.md), [Apache Parquet](./data-import-export/parquet-import.md), [Kafka](./data-import-export/kafka-stream.md), and [SQL](./data-import-export/execute-sql-queries.md).

The top-level package re-exports [`read_csv`](../reference/data-import-export/CSV/readCsv.md) from the [`csv`](#csv) module. The first example reads a CSV file with [`read_csv`](../reference/data-import-export/CSV/readCsv.md) and a Parquet file with the [`parquet`](#parquet) module's [`read`](../reference/data-import-export/Parquet/readTable.md).

```python order=t_from_csv,t_from_parquet
# Consume data from CSV and Parquet
from deephaven.parquet import read as read_pq
from deephaven import read_csv

t_from_csv = read_csv(
    "https://media.githubusercontent.com/media/deephaven/examples/main/Insurance/csv/insurance.csv"
)
t_from_parquet = read_pq("/data/examples/SensorData/parquet/SensorData_gzip.parquet")
```

The next example consumes a Kafka topic with [`consume`](../reference/data-import-export/Kafka/consume.md). It needs a running Kafka broker at `redpanda:9092`.

```python docker-config=kafka ticking-table order=null
# Consume data from Kafka
from deephaven.stream.kafka import consumer as kc
from deephaven import dtypes as dht

t = kc.consume(
    {"bootstrap.servers": "redpanda:9092"},
    "test.topic",
    table_type=kc.TableType.append(),
    key_spec=kc.KeyValueSpec.IGNORE,
    value_spec=kc.simple_spec("Command", dht.string),
)
```

The last example runs a SQL query against a PostgreSQL database with [`read_sql`](../reference/data-import-export/SQL/read_sql.md). It reads the connection details from environment variables. To run it, you need a PostgreSQL database and the [`connectorx`](https://pypi.org/project/connectorx/) package. Deephaven does not install it by default, so [install it](./install-and-use-python-packages.md) before you run the example.

```python skip-test
# Consume data from SQL
from deephaven.dbc import read_sql
import os

my_query = (
    "SELECT t_ts as Timestamp, CAST(t_id AS text) as Id, "
    "CAST(t_instrument as text) as Instrument, "
    "t_exchange as Exchange, t_price as Price, t_size as Size "
    "FROM crypto_trades"
)

username = os.environ["POSTGRES_USERNAME"]
password = os.environ["POSTGRES_PASSWORD"]
url = os.environ["POSTGRES_URL"]
port = os.environ["POSTGRES_PORT"]

sql_uri = f"postgresql://{url}:{port}/postgres?user={username}&password={password}"

crypto_trades = read_sql(conn=sql_uri, query=my_query, driver="connectorx")
```

### Filter data

You can [filter tables](./use-filters.md) by conditions or by row position, such as the rows at the top or bottom of a table. The following example filters with [`where`](../reference/table-operations/filter/where.md), using [query strings](./query-string-overview.md), including one that calls a [Python function](./python-functions.md). It then keeps the first or last rows with [`head`](../reference/table-operations/filter/head.md) and [`tail`](../reference/table-operations/filter/tail.md).

```python order=t_filtered,t_function_filtered,t_head,t_tail,t
from deephaven import empty_table


def filter_func(y: float, z: str) -> bool:
    return y < 2 or (y > 8 and z == "B")


t = empty_table(50).update(
    ["X = i", "Y = randomDouble(0.0, 10.0)", "Z = (X % 2 == 0) ? `A` : `B`"]
)

t_filtered = t.where(["Y > 5.0", "Z = `B`"])
t_function_filtered = t.where("filter_func(Y, Z)")
t_head = t.head(20)
t_tail = t.tail(5)
```

### Calculate grouped, cumulative, and rolling aggregations

The following example computes per-symbol averages with [`agg_by`](../reference/table-operations/group-and-aggregate/aggBy.md), which applies [combined aggregations](./combined-aggregations.md) such as [`avg`](../reference/table-operations/group-and-aggregate/AggAvg.md) and returns one row per group.

The example then uses [`update_by`](../reference/table-operations/update-by-operations/updateBy.md). Unlike [`agg_by`](../reference/table-operations/group-and-aggregate/aggBy.md), it keeps every row and adds the results as new columns. It adds two per-symbol [cumulative and rolling aggregations](./rolling-aggregations.md):

- [`cum_sum`](../reference/table-operations/update-by-operations/cum-sum.md) adds a running total of `X`.
- [`rolling_avg_tick`](../reference/table-operations/update-by-operations/rolling-avg-tick.md) averages `X` and `Y` over a window of rows: the current row and the four rows before it.

[Rolling aggregations](./rolling-aggregations.md) can also use a time window instead of a row window.

```python order=t_agg,t_updateby,t
from deephaven.updateby import cum_sum, rolling_avg_tick
from deephaven import empty_table
from deephaven import agg

t = empty_table(100).update(
    [
        "Sym = (i % 2 == 0) ? `A` : `B`",
        "X = randomDouble(0.0, 10.0)",
        "Y = randomDouble(50.0, 150.0)",
    ]
)

t_agg = t.agg_by(aggs=agg.avg(["AvgX = X", "AvgY = Y"]), by="Sym")
t_updateby = t.update_by(
    ops=[
        cum_sum(cols=["CumSumX = X"]),
        rolling_avg_tick(cols=["RollingAvgX = X", "RollingAvgY = Y"], rev_ticks=5),
    ],
    by="Sym",
)
```

### Join tables together

Deephaven offers a wide variety of ways to join tables together. Joins can be [exact or relational](./joins-exact-relational.md) or [inexact or time-series](./joins-timeseries-range.md).

Even [ticking tables](../conceptual/table-types.md) can be joined with no extra work. Joins on ticking tables use the same calls as joins on [static tables](../conceptual/table-types.md), and the result updates as the inputs change. The exception is [`range_join`](../reference/table-operations/join/range-join.md), which accepts only static tables.

The following example uses [`natural_join`](../reference/table-operations/join/natural-join.md) to add each employee's department details from `t_right` to the matching rows of `t_left`.

```python order=t,t_left,t_right
from deephaven import new_table
from deephaven.column import string_col, int_col
from deephaven.constants import NULL_INT

t_left = new_table(
    [
        string_col(
            "LastName", ["Rafferty", "Jones", "Steiner", "Robins", "Smith", "Rogers"]
        ),
        int_col("DeptID", [31, 33, 33, 34, 34, NULL_INT]),
        string_col(
            "Telephone",
            [
                "(303) 555-0162",
                "(303) 555-0149",
                "(303) 555-0184",
                "(303) 555-0125",
                None,
                None,
            ],
        ),
    ]
)

t_right = new_table(
    [
        int_col("DeptID", [31, 33, 34, 35]),
        string_col("DeptName", ["Sales", "Engineering", "Clerical", "Marketing"]),
        string_col("DeptManager", ["Martinez", "Williams", "Garcia", "Lopez"]),
        int_col("DeptGarage", [33, 52, 22, 45]),
    ]
)

t = t_left.natural_join(table=t_right, on=["DeptID"])
```

Rogers has no `DeptID`, so the department columns in that row are null. No employee belongs to Marketing, so it does not appear in the result.

### Convert to and from NumPy and pandas

[NumPy](https://numpy.org/) and [pandas](https://pandas.pydata.org/) are two of Python's most popular packages, and Deephaven Python queries often use them. The [`numpy`](#numpy) and [`pandas`](#pandas) modules convert tables to and from these libraries' data structures.

The following example converts a table to a NumPy array with [`to_numpy`](../reference/numpy/to-numpy.md) and to a DataFrame with [`to_pandas`](../reference/pandas/to-pandas.md). It then converts each one back with the NumPy [`to_table`](../reference/numpy/to-table.md) and the pandas [`to_table`](../reference/pandas/to-table.md). For more, see [Use NumPy in Deephaven queries](./use-numpy.md) and [Convert between Deephaven tables and pandas DataFrames](./use-pandas.md).

```python order=t,t_from_np,t_from_pd,:log
from deephaven import pandas as dhpd
from deephaven import numpy as dhnp
from deephaven import empty_table

t = empty_table(10).update("X = randomDouble(-10.0, 10.0)")
np_arr_from_table = dhnp.to_numpy(t)
df_from_table = dhpd.to_pandas(t)

print(np_arr_from_table)
print(df_from_table)

t_from_np = dhnp.to_table(np_arr_from_table, cols=["X"])
t_from_pd = dhpd.to_table(df_from_table)
```

### Export tables to Parquet, CSV, Kafka, and more

You can export table data to many formats, including [Parquet](./data-import-export/parquet-export.md), [CSV](./data-import-export/csv-export.md), [Kafka](./data-import-export/kafka-stream.md#write-to-a-kafka-stream), and [HTML](./data-import-export/html-export.md). The following code block writes a table to Parquet with the [`parquet`](#parquet) module's [`write`](../reference/data-import-export/Parquet/writeTable.md) and to CSV with [`write_csv`](../reference/data-import-export/CSV/writeCsv.md).

```python order=my_table
from deephaven.parquet import write as write_pq
from deephaven import empty_table
from deephaven import write_csv

my_table = empty_table(100).update(["X = 0.1 * i", "Y = sin(X)", "Z = cos(X)"])

write_pq(my_table, "/data/my_table.parquet")
write_csv(my_table, "/data/my_table.csv")
```

To share a table with other Deephaven servers without exporting it, use [Uniform Resource Identifiers (URIs)](./use-uris.md).

## What's in the `deephaven` package

Each of the following subsections, in alphabetical order, describes one submodule of the `deephaven` package and links to its Pydoc.

### `agg`

The [`agg`](/core/pydoc/code/deephaven.agg.html) submodule defines the [combined aggregations](./combined-aggregations.md) that table operations such as [`agg_by`](../reference/table-operations/group-and-aggregate/aggBy.md) apply.

### `appmode`

The [`appmode`](/core/pydoc/code/deephaven.appmode.html) submodule supports writing [Deephaven application mode](./application-mode.md) Python scripts.

### `arrow`

The [`arrow`](/core/pydoc/code/deephaven.arrow.html) submodule supports conversions to and from [PyArrow](https://arrow.apache.org/docs/python/) tables and Deephaven tables. See [Deephaven and PyArrow](./pyarrow.md).

### `barrage`

The [`barrage`](/core/pydoc/code/deephaven.barrage.html) submodule provides functions for accessing resources on remote Deephaven servers over [Barrage](../conceptual/what-is-barrage.md). See [Capture Python client tables with Barrage](./capture-tables.md) to use it with shared tickets.

### `calendar`

The [`calendar`](/core/pydoc/code/deephaven.calendar.html) submodule defines functions for working with [business calendars](./business-calendar.md).

### `column`

The [`column`](/core/pydoc/code/deephaven.column.html) submodule implements columns in tables. You use it mainly to create [new tables](./new-and-empty-table.md#new_table).

### `concurrency_control`

The [`concurrency_control`](/core/pydoc/code/deephaven.concurrency_control.html) submodule defines the [`Barrier`](../reference/query-language/types/Barrier.md) class and the [`ConcurrencyControl`](../reference/query-language/types/ConcurrencyControl.md) protocol. They give you explicit control over concurrency when [`select`](../reference/table-operations/select/select.md), [`update`](../reference/table-operations/select/update.md), and [`where`](../reference/table-operations/filter/where.md) evaluate [formulas](./formulas.md) and [filters](./filters.md). See the [parallelization guide](../conceptual/query-engine/parallelization.md).

### `configuration`

The [`configuration`](/core/pydoc/code/deephaven.configuration.html) submodule provides read access to Deephaven [configuration properties](./configuration/configuration-properties.md) as strings, numbers, or booleans.

### `constants`

The [`constants`](/core/pydoc/code/deephaven.constants.html) submodule defines the global constants, including Deephaven's special numerical values. Values include the maximum, minimum, NaN, null, and infinity values for different [data types](./data-types.md).

### `csv`

The [`csv`](/core/pydoc/code/deephaven.csv.html) submodule supports [reading an external CSV file](./data-import-export/csv-import.md) into a Deephaven table and [writing a table to a CSV file](./data-import-export/csv-export.md). The top-level `deephaven` package re-exports its [`read`](../reference/data-import-export/CSV/readCsv.md) and [`write`](../reference/data-import-export/CSV/writeCsv.md) functions as [`read_csv`](../reference/data-import-export/CSV/readCsv.md) and [`write_csv`](../reference/data-import-export/CSV/writeCsv.md).

### `dbc`

The [`dbc`](/core/pydoc/code/deephaven.dbc.html) submodule lets you [connect to and execute queries on external databases](./data-import-export/execute-sql-queries.md) from Deephaven.

### `dherror`

The [`dherror`](/core/pydoc/code/deephaven.dherror.html) submodule defines a custom exception for the Deephaven Python package. See the [triage errors guide](./triage-errors.md) for more information.

### `dtypes`

The [`dtypes`](/core/pydoc/code/deephaven.dtypes.html) submodule defines the [data types](../reference/python/deephaven-python-types.md) supported by the Deephaven engine.

### `execution_context`

The [`execution_context`](/core/pydoc/code/deephaven.execution_context.html) submodule lets you manage the Deephaven query [execution context](../conceptual/execution-context.md) on threads.

### `experimental`

The [`experimental`](/core/pydoc/code/deephaven.experimental.html) submodule contains experimental Deephaven features. Their interfaces can change between releases. Examples include [left outer joins](../reference/table-operations/join/left-outer-join.md), [full outer joins](../reference/table-operations/join/full-outer-join.md), [AWS S3 support](./data-import-export/parquet-import.md#from-s3), and [Iceberg integration](./data-import-export/iceberg.md).

### `filters`

The [`filters`](/core/pydoc/code/deephaven.filters.html) submodule implements filters that you can use in [filtering operations](./use-filters.md).

### `html`

The [`html`](/core/pydoc/code/deephaven.html.html) submodule supports [exporting Deephaven data in HTML format](./data-import-export/html-export.md).

### `jcompat`

The [`jcompat`](/core/pydoc/code/deephaven.jcompat.html) submodule provides Java compatibility support and convenience functions to create Java data structures from corresponding Python ones.

### `json`

The [`json`](/core/pydoc/code/deephaven.json.html) submodule describes the structure of JSON values declaratively. For example, to [read a Kafka topic in JSON format](./data-import-export/kafka-stream.md#read-kafka-topic-in-json-format), build a Jackson provider from a JSON value description with [`jackson.provider`](/core/pydoc/code/deephaven.json.jackson.html#deephaven.json.jackson.provider). Then pass the provider to [`object_processor_spec`](/core/pydoc/code/deephaven.stream.kafka.consumer.html#deephaven.stream.kafka.consumer.object_processor_spec).

### `learn`

The [`learn`](/core/pydoc/code/deephaven.learn.html) submodule provides utilities for efficient data transfer between tables and Python objects. It serves as a framework for [using machine learning libraries](./use-deephaven-learn.md) in Deephaven Python queries.

### `liveness_scope`

The [`liveness_scope`](/core/pydoc/code/deephaven.liveness_scope.html) submodule uses [liveness scopes](../conceptual/liveness-scope-concept.md) to control when the engine cleans up unreferenced nodes in the [query update graph](../conceptual/table-update-model.md), instead of relying only on garbage collection.

### `numpy`

The [`numpy`](/core/pydoc/code/deephaven.numpy.html) submodule supports the conversion between Deephaven tables and [NumPy arrays](https://numpy.org/doc/stable/reference/generated/numpy.array.html). See [Use NumPy in Deephaven queries](./use-numpy.md).

### `pandas`

The [`pandas`](/core/pydoc/code/deephaven.pandas.html) submodule supports the conversion between Deephaven tables and [pandas DataFrames](https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.html). See [Convert between Deephaven tables and pandas DataFrames](./use-pandas.md).

### `pandasplugin`

The [`pandasplugin`](/core/pydoc/code/deephaven.pandasplugin.html) submodule displays [pandas DataFrames](https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.html) as Deephaven tables in the UI.

### `parquet`

The [`parquet`](/core/pydoc/code/deephaven.parquet.html) submodule supports [reading external Parquet files into Deephaven tables](./data-import-export/parquet-import.md) and [writing Deephaven tables out as Parquet files](./data-import-export/parquet-export.md).

### `perfmon`

The [`perfmon`](/core/pydoc/code/deephaven.perfmon.html) submodule contains [tools to analyze performance of the Deephaven system and queries](./performance/performance-tables.md).

### `plot`

The [`plot`](/core/pydoc/code/deephaven.plot.html) submodule contains the framework for Deephaven's [built-in plotting API](./plotting/api-plotting.md). That API is no longer under active development. For most plotting tasks, use [Deephaven Express](/core/plotly/docs/) (`deephaven.plot.express`), which the `deephaven-plugin-plotly-express` package provides.

### `plugin`

The [`plugin`](/core/pydoc/code/deephaven.plugin.html) submodule contains the framework for [using plugins](./install-use-plugins.md) and [creating your own plugins](./create-plugins.md) in Deephaven. It comes from the `deephaven-plugin` package, which `deephaven-core` installs as a dependency.

### `plugin_authorization`

The [`plugin_authorization`](/core/pydoc/code/deephaven.plugin_authorization.html) submodule provides authorization helpers for Python plugins that send server objects, such as tables, to clients.

### `query_library`

The [`query_library`](/core/pydoc/code/deephaven.query_library.html) submodule lets you import Java classes or packages into the [query library](./built-in-functions.md#add-java-classes-to-the-query-library) so you can use them in queries.

### `replay`

The [`replay`](/core/pydoc/code/deephaven.replay.html) submodule provides support for [replaying historical data](./replay-data.md).

### `server`

The [`server`](/core/pydoc/code/deephaven.server.html) submodule provides the [`executors`](/core/pydoc/code/deephaven.server.executors.html) module, which lets Python code submit tasks to named JVM server executors. The two executors are `serial` and `concurrent`.

### `stream`

The [`stream`](/core/pydoc/code/deephaven.stream.html) submodule contains Deephaven's [Apache Kafka](./data-import-export/kafka-stream.md) integration, [table publishers](./table-publisher.md), and functions for converting between [table types](../conceptual/table-types.md), such as [`blink_to_append_only`](../reference/table-operations/create/blink-to-append-only.md).

### `systemic_obj_tracker`

The [`systemic_obj_tracker`](/core/pydoc/code/deephaven.systemic_obj_tracker.html) submodule controls [systemic object marking](../conceptual/systemic-object-marking.md), which determines the objects whose failures the engine reports to the server's error handler.

### `table`

The [`table`](/core/pydoc/code/deephaven.table.html) submodule implements these objects:

- The Deephaven table.
- The [partitioned table](./partitioned-tables.md) and the [partitioned table proxy](../reference/table-operations/partitioned-tables/proxy.md).
- The [rollup table](./rollup-table.md) and the [tree table](./tree-table.md).
- The [multi-join table](../reference/table-operations/join/multi-join.md).
- The [`TableDefinition`](/core/pydoc/code/deephaven.table.html#deephaven.table.TableDefinition) class, which describes a table's columns.

### `table_factory`

The [`table_factory`](/core/pydoc/code/deephaven.table_factory.html) submodule provides various ways to create Deephaven tables, including:

- [Empty tables](./new-and-empty-table.md#empty_table) and [new tables](./new-and-empty-table.md#new_table).
- [Time tables](./time-table.md) and [ring tables](./ring-table.md).
- [Input tables](./input-tables.md).
- [Function-generated tables](./function-generated-tables.md).
- [`DynamicTableWriter`](../reference/table-operations/create/DynamicTableWriter.md) objects.

### `table_listener`

The [`table_listener`](/core/pydoc/code/deephaven.table_listener.html) submodule implements Deephaven's [table listeners](./table-listeners-python.md).

### `time`

The [`time`](/core/pydoc/code/deephaven.time.html) submodule defines functions for handling Deephaven [date-time data](../conceptual/time-in-deephaven.md).

### `update_graph`

The [`update_graph`](/core/pydoc/code/deephaven.update_graph.html) submodule provides access to the locks of the [update graph](../conceptual/table-update-model.md). By default, Deephaven acquires the shared lock for you when a table operation on a refreshing table needs it. Hold a lock yourself when you need a consistent view of ticking data across several operations.

### `updateby`

The [`updateby`](/core/pydoc/code/deephaven.updateby.html) submodule builds the [cumulative and rolling aggregations](./rolling-aggregations.md) that the [`update_by`](../reference/table-operations/update-by-operations/updateBy.md) table operation uses.

### `uri`

The [`uri`](/core/pydoc/code/deephaven.uri.html) submodule implements tools for resolving [Uniform Resource Identifiers (URIs)](./use-uris.md) into objects.

## Related documentation

- [Deephaven's table types](../conceptual/table-types.md)
- [Table operations overview](../conceptual/table-operations-overview.md)
- [What is Barrage?](../conceptual/what-is-barrage.md)
- [Parallelizing queries](../conceptual/query-engine/parallelization.md)
- [Execution Context](../conceptual/execution-context.md)
- [Liveness scopes](../conceptual/liveness-scope-concept.md)
- [Incremental update model](../conceptual/table-update-model.md)
- [Systemic object marking](../conceptual/systemic-object-marking.md)
- [Work with time](../conceptual/time-in-deephaven.md)
- [Crash Course](../getting-started/crash-course/overview.md)
- [Python client quickstart](../getting-started/pyclient-quickstart.md)
- [Install packages](./install-packages.md)
- [Install and use Python packages](./install-and-use-python-packages.md)
- [Create static tables](./new-and-empty-table.md)
- [Create a time table](./time-table.md)
- [Read CSV or other delimited files](./data-import-export/csv-import.md)
- [Read Parquet files into Deephaven tables](./data-import-export/parquet-import.md)
- [Connect to a Kafka stream](./data-import-export/kafka-stream.md)
- [Execute SQL queries in Deephaven](./data-import-export/execute-sql-queries.md)
- [Export Deephaven Tables to Parquet Files](./data-import-export/parquet-export.md)
- [Export data to CSV or other delimited files](./data-import-export/csv-export.md)
- [Convert tables to HTML strings](./data-import-export/html-export.md)
- [Use URIs to share tables](./use-uris.md)
- [Filter table data](./use-filters.md)
- [An overview of query strings](./query-string-overview.md)
- [Formulas](./formulas.md)
- [Filters](./filters.md)
- [Python functions in query strings](./python-functions.md)
- [Multi-aggregation](./combined-aggregations.md)
- [Rolling aggregations](./rolling-aggregations.md)
- [Exact and relational joins](./joins-exact-relational.md)
- [Inexact, time-series, and range joins](./joins-timeseries-range.md)
- [Use NumPy in Deephaven queries](./use-numpy.md)
- [Convert between Deephaven tables and pandas DataFrames](./use-pandas.md)
- [Deephaven and PyArrow](./pyarrow.md)
- [Application Mode](./application-mode.md)
- [Capture Python client tables with Barrage](./capture-tables.md)
- [Work with calendars](./business-calendar.md)
- [Configuration properties reference](./configuration/configuration-properties.md)
- [Data types in Deephaven and Python](./data-types.md)
- [Triage errors in queries](./triage-errors.md)
- [Iceberg and Deephaven](./data-import-export/iceberg.md)
- [Use deephaven.learn for AI/ML applications](./use-deephaven-learn.md)
- [How to use Deephaven's performance tables for system monitoring](./performance/performance-tables.md)
- [Built-in plotting API](./plotting/api-plotting.md)
- [Install and use plugins](./install-use-plugins.md)
- [Create your own plugin](./create-plugins.md)
- [Built-in query language functions](./built-in-functions.md)
- [Replay data from static tables](./replay-data.md)
- [Write data to an in-memory, real-time table](./table-publisher.md)
- [Create and use partitioned tables](./partitioned-tables.md)
- [Create a hierarchical rollup table programmatically](./rollup-table.md)
- [Create a hierarchical tree table](./tree-table.md)
- [Create and use input tables](./input-tables.md)
- [Generate tables with Python functions](./function-generated-tables.md)
- [Create a ring table](./ring-table.md)
- [Listen to ticking tables](./table-listeners-python.md)
- [Data types in the Deephaven Python dtypes package](../reference/python/deephaven-python-types.md)
- [`agg_by`](../reference/table-operations/group-and-aggregate/aggBy.md)
- [`avg`](../reference/table-operations/group-and-aggregate/AggAvg.md)
- [`Barrier`](../reference/query-language/types/Barrier.md)
- [`blink_to_append_only`](../reference/table-operations/create/blink-to-append-only.md)
- [`ConcurrencyControl`](../reference/query-language/types/ConcurrencyControl.md)
- [`consume`](../reference/data-import-export/Kafka/consume.md)
- [`cum_sum`](../reference/table-operations/update-by-operations/cum-sum.md)
- [`double_col`](../reference/table-operations/create/doubleCol.md)
- [`DynamicTableWriter`](../reference/table-operations/create/DynamicTableWriter.md)
- [`empty_table`](../reference/table-operations/create/emptyTable.md)
- [`full_outer_join`](../reference/table-operations/join/full-outer-join.md)
- [`head`](../reference/table-operations/filter/head.md)
- [`int_col`](../reference/table-operations/create/intCol.md)
- [`left_outer_join`](../reference/table-operations/join/left-outer-join.md)
- [`multi_join`](../reference/table-operations/join/multi-join.md)
- [`natural_join`](../reference/table-operations/join/natural-join.md)
- [`new_table`](../reference/table-operations/create/newTable.md)
- [`proxy`](../reference/table-operations/partitioned-tables/proxy.md)
- [`range_join`](../reference/table-operations/join/range-join.md)
- [`read` (Parquet)](../reference/data-import-export/Parquet/readTable.md)
- [`read_csv`](../reference/data-import-export/CSV/readCsv.md)
- [`read_sql`](../reference/data-import-export/SQL/read_sql.md)
- [`rolling_avg_tick`](../reference/table-operations/update-by-operations/rolling-avg-tick.md)
- [`select`](../reference/table-operations/select/select.md)
- [`string_col`](../reference/table-operations/create/stringCol.md)
- [`tail`](../reference/table-operations/filter/tail.md)
- [`time_table`](../reference/table-operations/create/timeTable.md)
- [`to_numpy`](../reference/numpy/to-numpy.md)
- [`to_pandas`](../reference/pandas/to-pandas.md)
- [`to_table` (NumPy)](../reference/numpy/to-table.md)
- [`to_table` (pandas)](../reference/pandas/to-table.md)
- [`update`](../reference/table-operations/select/update.md)
- [`update_by`](../reference/table-operations/update-by-operations/updateBy.md)
- [`where`](../reference/table-operations/filter/where.md)
- [`write` (Parquet)](../reference/data-import-export/Parquet/writeTable.md)
- [`write_csv`](../reference/data-import-export/CSV/writeCsv.md)
- [Pydoc](/core/pydoc/code/deephaven.html)
