---
title: Deephaven's Python package
---

The Deephaven [Python package](https://pypi.org/project/deephaven-core/) is how users interact with the Deephaven engine. Deephaven's [Python API](/core/pydoc/) offers various cool ways for users to manipulate their data. This guide gives a brief overview of just what you can do.

## What can you do with `deephaven`?

In short: a lot. Here are a few cool things that only scratch the surface:

### Create tables from scratch

Tables can be created from scratch in a variety of ways. The code below creates tables with [`empty_table`](../reference/table-operations/create/emptyTable.md), [`new_table`](../reference/table-operations/create/newTable.md), and [`time_table`](../reference/table-operations/create/timeTable.md).

```python order=t1_static,t2_static,t_ticking
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

Ingest data into tables from external sources such as [CSV](./data-import-export/csv-import.md), [Apache Parquet](./data-import-export/parquet-import.md), [Kafka](./data-import-export/kafka-stream.md), and [SQL](./data-import-export/execute-sql-queries.md):

```python order=t_from_csv,t_from_parquet
# Consume data from CSV and Parquet
from deephaven.parquet import read as read_pq
from deephaven import read_csv

t_from_csv = read_csv(
    "https://media.githubusercontent.com/media/deephaven/examples/main/Insurance/csv/insurance.csv"
)
t_from_parquet = read_pq("/data/examples/SensorData/parquet/SensorData_gzip.parquet")
```

```python docker-config=kafka
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

```python skip-test
# Consume data from SQL
from deephaven.dbc import read_sql
import os

my_query = "SELECT t_ts as Timestamp, CAST(t_id AS text) as Id, " +
    "CAST(t_instrument as text) as Instrument, " +
    "t_exchange as Exchange, t_price as Price, t_size as Size " +
    "FROM CRYPTO TRADES"

username = os.environ["POSTGRES_USERNAME"]
password = os.environ["POSTGRES_PASSWORD"]
url = os.environ["POSTGRES_URL"]
port = os.environ["POSTGRES_PORT"]

sql_uri = f"postgresql://{url}:{port}/postgres?user={username}&password={password}"

crypto_trades = read_sql(conn=sql_uri, query=my_query, driver="connectorx")
```

### Export tables to Parquet, CSV, Kafka, and more

Table data can be exported to a wide variety of formats including [Parquet](./data-import-export/parquet-import.md), [CSV](./data-import-export/csv-export.md), [Kafka](./data-import-export/kafka-stream.md#write-to-a-kafka-stream), [Uniform Resource Identifiers (URIs)](./use-uris.md), and many more. The following code block specifically writes a table to CSV and Parquet for later use.

```python order=my_table
from deephaven.parquet import write as write_pq
from deephaven import empty_table
from deephaven import write_csv

my_table = empty_table(100).update(["X = 0.1 * i", "Y = sin(X)", "Z = cos(X)"])

write_pq(my_table, "/data/my_table.parquet")
write_csv(my_table, "/data/my_table.csv")
```

### Filter data

Data can be [filtered out of tables](./use-filters.md) based on conditions being met or by row numbers, such as those at the top or bottom of a table.

```python order=t_filtered,t_function_filtered,t_head,t_tail,t
from deephaven import empty_table


def filter_func(y: float, z: str) -> bool:
    if y < 2 or y > 8 and z == "B":
        return True
    else:
        return False


t = empty_table(50).update(
    ["X = i", "Y = randomDouble(0.0, 10.0)", "Z = (X % 2 == 0) ? `A` : `B`"]
)

t_filtered = t.where(["Y > 5.0", "Z = `B`"])
t_function_filtered = t.where("filter_func(Y, Z)")
t_head = t.head(20)
t_tail = t.tail(5)
```

### Calculate cumulative and rolling aggregations

Aggregations can be cumulative or windowed (by either time or rows).

```python order=t_agg,t_updateby,t
from deephaven.updateby import rolling_avg_tick
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
    ops=rolling_avg_tick(cols=["RollingAvgX = X", "RollingAvgY = Y"], rev_ticks=5),
    by="Sym",
)
```

### Join tables together

Deephaven offers a wide variety of ways to join tables together. Joins can be [exact or relational](./joins-exact-relational.md) or [inexact or time-series](./joins-timeseries-range.md). Even ticking tables can be joined with no extra work.

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

### Convert to and from NumPy and Pandas

[NumPy](https://numpy.org/) and [Pandas](https://pandas.pydata.org/) are two of Python's most popular packages. They get used often in Deephaven Python queries. So, there are mechanisms available that make converting between tables and Python data structures a breeze.

```python order=t,t_from_np,t_from_pd,:log
from deephaven import pandas as dhpd
from deephaven import numpy as dhnp
from deephaven import empty_table
import pandas as pd
import numpy as np

t = empty_table(10).update("X = randomDouble(-10.0, 10.0)")
np_arr_from_table = dhnp.to_numpy(t)
df_from_table = dhpd.to_pandas(t)

print(np_arr_from_table)
print(df_from_table)

t_from_np = dhnp.to_table(np_arr_from_table, cols="X")
t_from_pd = dhpd.to_table(df_from_table)
```

## What's in the `deephaven` package

The `deephaven` package is used to interact with tables and other Deephaven objects in queries. Each of the following subsections discusses a single submodule within the `deephaven` package. They are presented in alphabetical order.

### `agg`

The [`agg`](/core/pydoc/code/deephaven.agg.html) submodule defines the [combined aggregations](./combined-aggregations.md) that are usable in queries.

### `appmode`

The [`appmode`](/core/pydoc/code/deephaven.appmode.html) submodule supports writing [Deephaven application mode](./application-mode.md) Python scripts.

### `arrow`

The [`arrow`](/core/pydoc/code/deephaven.arrow.html) submodule supports conversions to and from [PyArrow](https://arrow.apache.org/docs/python/) tables and Deephaven tables.

### `barrage`

<!-- TODO: Link to shared tickets user guide https://github.com/deephaven/deephaven.io/pull/3919 -->

The [`barrage`](/core/pydoc/code/deephaven.barrage.html) submodule provides functions for accessing resources on remote Deephaven servers.

### `calendar`

The [`calendar`](/core/pydoc/code/deephaven.calendar.html) submodule defines functions for working with [business calendars](./business-calendar.md).

### `column`

The [`column`](/core/pydoc/code/deephaven.column.html) submodule implements columns in tables. It is primarily used when creating [new tables](./new-and-empty-table.md#new_table).

### `constants`

The [`constants`](/core/pydoc/code/deephaven.constants.html) submodule defines the global constants, including Deephaven's special numerical values. Values include the maximum, minimum, NaN, null, and infinity values for different [data types](./data-types.md).

### `csv`

The [`csv`](/core/pydoc/code/deephaven.csv.html) submodule supports [reading an external CSV file](./data-import-export/csv-import.md) into a Deephaven table and [writing a table to a CSV file](./data-import-export/csv-export.md).

### `dbc`

The [`dbc`](/core/pydoc/code/deephaven.dbc.html) submodule enables users to [connect to and execute queries on external databases](./data-import-export/execute-sql-queries.md) in Deephaven.

### `dherror`

The [`dherror`](/core/pydoc/code/deephaven.dherror.html) submodule defines a custom exception for the Deephaven Python package. See the [triage errors guide](./triage-errors.md) for more information.

### `dtypes`

The [`dtypes`](/core/pydoc/code/deephaven.dtypes.html) submodule defines the [data types](../reference/python/deephaven-python-types.md) supported by the Deephaven engine.

### `execution_context`

The [`execution_context`](/core/pydoc/code/deephaven.execution_context.html) submodule gives users the ability to directly manage the Deephaven query [execution context](../conceptual/execution-context.md) on threads.

### `experimental`

The [`experimental`](/core/pydoc/code/deephaven.experimental.html) submodule contains experimental features. Current experimental features include [outer joins](../reference/table-operations/join/left-outer-join.md) and [AWS S3 support](./data-import-export/parquet-import.md#from-s3).

### `filters`

The [`filters`](/core/pydoc/code/deephaven.filters.html) submodule implements various filters that can be used in [filtering operations](./use-filters.md).

### `html`

The [`html`](/core/pydoc/code/deephaven.html.html) submodule supports exporting Deephaven data in HTML format.

### `jcompat`

The [`jcompat`](/core/pydoc/code/deephaven.jcompat.html) submodule provides Java compatibility support and convenience functions to create Java data structures from corresponding Python ones.

### `learn`

The [`learn`](/core/pydoc/code/deephaven.learn.html) submodule provides utilities for efficient data transfer between tables and Python objects. It serves as a framework for [using machine learning libraries](./use-deephaven-learn.md) in Deephaven Python queries.

### `liveness_scope`

The [`liveness_scope`](/core/pydoc/code/deephaven.liveness_scope.html) submodule gives a finer degree of control over when to clean up unreferenced nodes in the [query update graph](../conceptual/table-update-model.md) instead of solely relying on garbage collection.

### `numpy`

The [`numpy`](/core/pydoc/code/deephaven.numpy.html) submodule supports the conversion between Deephaven tables and [NumPy arrays](https://numpy.org/doc/stable/reference/generated/numpy.array.html).

### `pandas`

The [`pandas`](/core/pydoc/code/deephaven.pandas.html) submodule supports the conversion between Deephaven tables and [Pandas DataFrames](https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.html).

### `pandasplugin`

The [`pandasplugin`](/core/pydoc/code/deephaven.pandasplugin.html) submodule is used to display [Pandas DataFrames](https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.html) as Deephaven tables in the UI.

### `parquet`

The [`parquet`](/core/pydoc/code/deephaven.parquet.html) submodule supports [reading external Parquet files into Deephaven](./data-import-export/parquet-import.md) tables and [writing Deephaven tables out as Parquet files](./data-import-export/parquet-export.md).

### `perfmon`

The [`perfmon`](/core/pydoc/code/deephaven.perfmon.html) submodule contains [tools to analyze performance of the Deephaven system and queries](./performance/performance-tables.md).

### `plot`

The [`plot`](/core/pydoc/code/deephaven.plot.html) submodule contains the framework for Deephaven's [built-in plotting API](./plotting/api-plotting.md#category).

### `plugin`

The [`plugin`](/core/pydoc/code/deephaven.plugin.html) submodule contains the framework for [using plugins](./create-plugins.md) and [creating your own plugins](./create-plugins.md) in Deephaven.

### `query_library`

The [`query_library`](/core/pydoc/code/deephaven.query_library.html) submodule allows users to import Java classes or packages into the query library, which they can then use in queries.

### `replay`

The [`replay`](/core/pydoc/code/deephaven.replay.html) submodule provides support for [replaying historical data](./replay-data.md).

### `server`

The [`server`](/core/pydoc/code/deephaven.server.html) submodule contains the framework for [running Python operations from within a JVM](../conceptual/python-java-boundary.md).

### `stream`

The [`stream`](/core/pydoc/code/deephaven.stream.html) submodule contains Deephaven's [Apache Kafka](./data-import-export/kafka-stream.md) integration, [Table Publisher](./table-publisher.md), and a utility for converting between [table types](../conceptual/table-types.md).

### `table`

The [`table`](/core/pydoc/code/deephaven.table.html) submodule implements the Deephaven table, [partitioned table](./partitioned-tables.md), and partitioned table proxy objects.

### `table_factory`

The [`table_factory`](/core/pydoc/code/deephaven.table_factory.html) submodule provides various ways to create Deephaven tables, including [empty tables](./new-and-empty-table.md#empty_table), [input tables](./input-tables.md), [new tables](./new-and-empty-table.md#new_table), [function-generated tables](./function-generated-tables.md), [time tables](./time-table.md), [ring tables](../conceptual/table-types.md#specialization-4-ring), and [DynamicTableWriter](../how-to-guides/table-publisher.md#dynamictablewriter).

### `table_listener`

The [`table_listener`](/core/pydoc/code/deephaven.table_listener.html) submodule implements Deephaven's [table listeners](./table-listeners-python.md).

### `time`

The [`time`](/core/pydoc/code/deephaven.time.html) submodule defines functions for handling Deephaven [date-time data](../conceptual/time-in-deephaven.md).

### `update_graph`

The [`update_graph`](/core/pydoc/code/deephaven.update_graph.html) submodule provides access to the update graph's locks that must be acquired to perform certain table operations.

### `updateby`

The [`updateby`](/core/pydoc/code/deephaven.updateby.html) submodule supports building various [cumulative and rolling aggregations](./rolling-aggregations.md) to be used in the [`update_by`](../reference/table-operations/update-by-operations/updateBy.md) table operation.

### `uri`

The [`uri`](/core/pydoc/code/deephaven.uri.html) submodule implements tools for resolving [Uniform Resource Identifiers (URIs)](./use-uris.md) into objects.

## Related documentation

- [Pydoc](/core/pydoc/code/deephaven.html)
