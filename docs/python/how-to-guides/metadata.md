---
title: Access table metadata
---

This guide shows how to use the properties and methods of Deephaven's [`Table`](/core/pydoc/code/deephaven.table.html#deephaven.table.Table) class to access the metadata for your table, and how to use the [`table_diff`](../reference/table-operations/metadata/table_diff.md) function to compare two tables.

A table's metadata describes the table itself: its [table type](../conceptual/table-types.md) and size, whether it is [refreshing](../conceptual/table-types.md#standard-streaming-tables), and the data type of each column. You might use it to confirm that a column is an `int` or a `double`, or to check whether a column is a partitioning column. A partitioning column gets its values from how the data is laid out in storage, not from the data files. For example, the `key=value` directory names of a [key-value partitioned Parquet directory](./data-import-export/parquet-formats.md#key-value-partitioned-directories) become partitioning columns.

## `meta_table`

The [`meta_table`](../reference/table-operations/metadata/meta_table.md) property creates a new table that contains the table's metadata. Specifically, this table contains one row for every column in the original table, with four columns: `Name`, `DataType`, `ColumnType`, and `IsPartitioning`. `ColumnType` is `Normal` for a normal column or `Partitioning` for a partitioning column, and `IsPartitioning` gives the same information as a `true`/`false` value.

```python syntax
result = source.meta_table
```

Let's create a table of weather data for Miami, Florida.

```python test-set=1
from deephaven import new_table
from deephaven.column import string_col, int_col, double_col

miami = new_table(
    [
        string_col(
            "Month",
            [
                "Jan",
                "Feb",
                "Mar",
                "Apr",
                "May",
                "Jun",
                "Jul",
                "Aug",
                "Sep",
                "Oct",
                "Nov",
                "Dec",
            ],
        ),
        int_col("Temp", [60, 62, 65, 68, 73, 76, 77, 77, 76, 74, 68, 63]),
        double_col(
            "Rain",
            [1.62, 2.25, 3.00, 3.14, 5.34, 9.67, 6.50, 8.88, 9.86, 6.33, 3.27, 2.04],
        ),
    ]
)
```

We can access the metadata as follows:

```python test-set=1
meta = miami.meta_table
```

The `meta` table has one row for each of the `Month`, `Temp`, and `Rain` columns of `miami`. Its `DataType` column shows that `Temp` is an `int` and `Rain` is a `double`. Metadata is most useful for a table you're unfamiliar with, but even here it confirms each column's type at a glance.

Metadata also shows how an operation changes column types. Let's create a table of weather data for Miami over three dates in January, then [average](../reference/table-operations/group-and-aggregate/avgBy.md) the high and low temperatures by day.

```python test-set=2 order=miami_jan,avg_temp
from deephaven import new_table
from deephaven.column import string_col, int_col

miami_jan = new_table(
    [
        string_col("Day", ["Jan 1", "Jan 1", "Jan 2", "Jan 2", "Jan 3", "Jan 3"]),
        int_col("Temp", [45, 62, 48, 63, 39, 59]),
    ]
)

avg_temp = miami_jan.avg_by(by=["Day"])
```

The `Temp` column starts as an [`int` column](../reference/table-operations/create/intCol.md), but [`avg_by`](../reference/table-operations/group-and-aggregate/avgBy.md) turns it into a [`double` column](../reference/table-operations/create/doubleCol.md). We can see this by hovering over the column header in the UI, and also by accessing the table's metadata.

```python test-set=2 order=meta
meta = avg_temp.meta_table
```

## Table properties

Like [`meta_table`](#meta_table), the following are properties, so access them without parentheses. Each example prints the value the property returns.

### `columns`

For a full list of a table's column definitions, use [`columns`](../reference/table-operations/metadata/columns.md):

```python test-set=1 order=:log
print(miami.columns)
```

### `column_names`

For a list of a table's column names, use [`column_names`](/core/pydoc/code/deephaven.table.html#deephaven.table.Table.column_names):

```python test-set=1 order=:log
print(miami.column_names)
```

### `definition`

To get a table's [`TableDefinition`](/core/pydoc/code/deephaven.table.html#deephaven.table.TableDefinition), a mapping from each column name to its column definition, use [`definition`](/core/pydoc/code/deephaven.table.html#deephaven.table.Table.definition):

```python test-set=1 order=:log
print(miami.definition)
```

### `is_blink`

To see whether a table is a [blink table](../conceptual/table-types.md#specialization-3-blink), use [`is_blink`](../reference/table-operations/metadata/is_blink.md). The static `miami` table is not a blink table. A [time table](../reference/table-operations/create/timeTable.md) created with `blink_table=True` is, because it keeps only the rows added in the current update cycle:

```python test-set=1 ticking-table order=:log
from deephaven import time_table

live_blink = time_table("PT1s", blink_table=True)

print(miami.is_blink, live_blink.is_blink)
```

### `is_failed`

To see whether a table is in a failure state and no longer usable, use [`is_failed`](/core/pydoc/code/deephaven.table.html#deephaven.table.Table.is_failed):

```python test-set=1 order=:log
print(miami.is_failed)
```

### `is_flat`

To see whether a table is guaranteed to be flat, meaning its [row keys](../conceptual/table-update-model.md#describing-table-updates) run contiguously from 0 to the number of rows minus 1, use [`is_flat`](../reference/table-operations/metadata/is_flat.md):

```python test-set=1 order=:log
print(miami.is_flat)
```

### `is_refreshing`

To see whether a table is [refreshing](../conceptual/table-types.md#standard-streaming-tables), meaning its rows can change as new data arrives, use [`is_refreshing`](../reference/table-operations/metadata/is_refreshing.md). The `miami` table never changes, so it is not refreshing. A [time table](../reference/table-operations/create/timeTable.md) adds a row as time passes, so it is:

```python test-set=1 ticking-table order=:log
from deephaven import time_table

live = time_table("PT1s")

print(miami.is_refreshing, live.is_refreshing)
```

### `size`

To see the number of rows in a table, use [`size`](../reference/table-operations/metadata/size.md):

```python test-set=1 order=:log
print(miami.size)
```

### `update_graph`

To get the [UpdateGraph](../conceptual/dag.md#update-graph-cycles) that schedules a table's updates, for example to pass to the locking functions in [`deephaven.update_graph`](/core/pydoc/code/deephaven.update_graph.html), use [`update_graph`](../reference/table-operations/metadata/update_graph.md):

```python test-set=1 order=:log
print(miami.update_graph)
```

## Table methods

The following methods return information about a table: its attributes, whether it has given columns, and a string preview of its first rows. Unlike the properties above, call them with parentheses.

### `attributes`

Table attributes are metadata that the engine attaches to a table, such as [`AddOnly`](/core/javadoc/io/deephaven/engine/table/Table.html#ADD_ONLY_TABLE_ATTRIBUTE) and [`AppendOnly`](/core/javadoc/io/deephaven/engine/table/Table.html#APPEND_ONLY_TABLE_ATTRIBUTE), which mark a table whose rows are only ever added or appended. The [`attributes`](../reference/table-operations/metadata/attributes.md) method returns all of a table's defined attributes as a `dict`.

The `miami` table has no attributes, so [`attributes`](../reference/table-operations/metadata/attributes.md) returns an empty `dict`. An [input table](../reference/table-operations/create/input-table.md) created from it has the [`InputTable`](/core/javadoc/io/deephaven/engine/table/Table.html#INPUT_TABLE_ATTRIBUTE) attribute. Because it has no key columns, it is append-only, so it also has the [`AddOnly`](/core/javadoc/io/deephaven/engine/table/Table.html#ADD_ONLY_TABLE_ATTRIBUTE) and [`AppendOnly`](/core/javadoc/io/deephaven/engine/table/Table.html#APPEND_ONLY_TABLE_ATTRIBUTE) attributes.

```python test-set=1 order=:log
from deephaven import input_table

miami_input = input_table(init_table=miami)

print(miami.attributes())
print(miami_input.attributes())
```

### `has_columns`

Use [`has_columns`](../reference/table-operations/metadata/has_columns.md) to check whether a table contains one or more named columns. It returns `True` if the table contains every column you name, or `False` if it lacks _any_ of them.

```python test-set=1 order=:log
print(miami.has_columns("Month"))
print(miami.has_columns(["Month", "NotAColumnName"]))
```

### `to_string`

Use [`to_string`](../reference/table-operations/metadata/to_string.md) to return the first _n_ rows of a table as a pipe-delimited string.

```python syntax
source.to_string(
    num_rows: int = 10,
    cols: Optional[Union[str, Sequence[str]]] = None
) -> str
```

- `num_rows` is the number of rows from the beginning of the table to return. The default is 10.
- `cols` is the column name(s) to include in the string. The default is `None`, which includes all columns.

```python test-set=1 order=:log
print(miami.to_string(4))
```

## `table_diff`

Use the [`table_diff`](../reference/table-operations/metadata/table_diff.md) function from the [`deephaven.table`](/core/pydoc/code/deephaven.table.html) module to compare two tables and return the differences between them as a string. It returns an empty string if the tables are the same.

[`table_diff`](../reference/table-operations/metadata/table_diff.md) checks the tables in this order:

- Table size.
- Schema: missing or extra columns, column order, and column types.
- Data, column by column, for every column with the same name and type in both tables. It records only the first difference in each column.

It stops once it has recorded `max_diffs` differences.

To illustrate, let's compare two tables whose `B` and `C` columns differ on alternating rows.

```python order=:log
from deephaven import empty_table
from deephaven.table import table_diff

# Create some tables
t1 = empty_table(10).update(["A = i", "B = i", "C = i"])
t2 = empty_table(10).update(
    ["A = i", "B = i % 2 == 0 ? i : i + 1", "C = i % 2 == 0 ? i + 1 : i"]
)

# Get the diff string
d = table_diff(t1, t2, max_diffs=10).split("\n")

print(d)
```

The two tables have the same schema, so the only differences are in the data. [`table_diff`](../reference/table-operations/metadata/table_diff.md) treats the first table as the actual result and the second as the expected one, so each line reads "encountered _`t1` value_ expected _`t2` value_". It reports that column `B` first differs at row 1 and column `C` at row 0. Column `A` is identical in both tables.

## Related documentation

- [Create a new table](./new-and-empty-table.md#new_table)
- [How to perform dedicated aggregations for groups](./dedicated-aggregations.md)
- [Create and use input tables](./input-tables.md)
- [Create a time table](./time-table.md)
- [Deephaven's table types](../conceptual/table-types.md)
- [Deephaven's live Directed-Acyclic-Graph (DAG)](../conceptual/dag.md)
- [Supported Parquet formats](./data-import-export/parquet-formats.md)
- [`meta_table`](../reference/table-operations/metadata/meta_table.md)
- [`attributes`](../reference/table-operations/metadata/attributes.md)
- [`avg_by`](../reference/table-operations/group-and-aggregate/avgBy.md)
- [`input_table`](../reference/table-operations/create/input-table.md)
- [`columns`](../reference/table-operations/metadata/columns.md)
- [`has_columns`](../reference/table-operations/metadata/has_columns.md)
- [`is_blink`](../reference/table-operations/metadata/is_blink.md)
- [`is_flat`](../reference/table-operations/metadata/is_flat.md)
- [`is_refreshing`](../reference/table-operations/metadata/is_refreshing.md)
- [`size`](../reference/table-operations/metadata/size.md)
- [`table_diff`](../reference/table-operations/metadata/table_diff.md)
- [`time_table`](../reference/table-operations/create/timeTable.md)
- [`to_string`](../reference/table-operations/metadata/to_string.md)
- [`update_graph`](../reference/table-operations/metadata/update_graph.md)
- [Pydoc](/core/pydoc/code/deephaven.table.html#deephaven.table.Table)
