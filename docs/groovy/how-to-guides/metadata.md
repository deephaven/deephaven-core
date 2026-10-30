---
title: Access table metadata
---

This guide shows how to use the methods of Deephaven [tables](/core/javadoc/io/deephaven/engine/table/Table.html) to access the metadata for your table, and how to use the static [`string`](#string) and [`diff`](../reference/table-operations/metadata/diff.md) methods from [`TableTools`](/core/javadoc/io/deephaven/engine/util/TableTools.html) to preview a table and compare two tables.

A table's metadata describes the table itself: its [table type](../conceptual/table-types.md) and size, whether it is [refreshing](../conceptual/table-types.md#standard-streaming-tables), and the data type of each column. You might use it to confirm that a column is an `int` or a `double`, or to check whether a column is a partitioning column. A partitioning column gets its values from how the data is laid out in storage, not from the data files. For example, the `key=value` directory names of a [key-value partitioned Parquet directory](./data-import-export/parquet-formats.md#key-value-partitioned-directories) become partitioning columns.

## `meta`

The [`meta`](../reference/table-operations/metadata/meta.md) method creates a new table that contains the table's metadata. Specifically, this table contains one row for every column in the original table, with four columns: `Name`, `DataType`, `ColumnType`, and `IsPartitioning`. `ColumnType` is `Normal` for a normal column or `Partitioning` for a partitioning column, and `IsPartitioning` gives the same information as a `true`/`false` value.

```groovy syntax
result = source.meta()
```

Let's create a table of weather data for Miami, Florida.

```groovy test-set=2
miami = newTable(
    stringCol("Month", "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"),
    intCol("Temp", 60, 62, 65, 68, 73, 76, 77, 77, 76, 74, 68, 63),
    doubleCol("Rain", 1.62, 2.25, 3.00, 3.14, 5.34, 9.67, 6.50, 8.88, 9.86, 6.33, 3.27, 2.04),
)
```

We can access the metadata as follows:

```groovy test-set=2
meta = miami.meta()
```

The `meta` table has one row for each of the `Month`, `Temp`, and `Rain` columns of `miami`. Its `DataType` column shows that `Temp` is an `int` and `Rain` is a `double`. Metadata is most useful for a table you're unfamiliar with, but even here it confirms each column's type at a glance.

Metadata also shows how an operation changes column types. Let's create a table of weather data for Miami over three dates in January, then [average](../reference/table-operations/group-and-aggregate/avgBy.md) the high and low temperatures by day.

```groovy test-set=3 order=miamiJan,avgTemp
miamiJan = newTable(
    stringCol("Day", "Jan 1", "Jan 1", "Jan 2", "Jan 2", "Jan 3", "Jan 3"),
    intCol("Temp", 45, 62, 48, 63, 39, 59),
)

avgTemp = miamiJan.avgBy("Day")
```

The `Temp` column starts as an [`int` column](../reference/table-operations/create/intCol.md), but [`avgBy`](../reference/table-operations/group-and-aggregate/avgBy.md) turns it into a [`double` column](../reference/table-operations/create/doubleCol.md). We can see this by hovering over the column header in the UI, and also by accessing the table's metadata.

```groovy test-set=3 order=meta
meta = avgTemp.meta()
```

## Attributes

Table attributes are metadata that the engine attaches to a table, such as [`AddOnly`](/core/javadoc/io/deephaven/engine/table/Table.html#ADD_ONLY_TABLE_ATTRIBUTE) and [`AppendOnly`](/core/javadoc/io/deephaven/engine/table/Table.html#APPEND_ONLY_TABLE_ATTRIBUTE), which mark a table whose rows are only ever added or appended. The following methods read a table's attributes.

### `getAttribute`

Use the [`getAttribute`](../reference/table-operations/metadata/getAttribute.md) method to get the value of a specific table attribute.

The example below creates an append-only [input table](../reference/table-operations/create/InputTable.md) from `miami`. The input table has the [`InputTable`](/core/javadoc/io/deephaven/engine/table/Table.html#INPUT_TABLE_ATTRIBUTE), [`AddOnly`](/core/javadoc/io/deephaven/engine/table/Table.html#ADD_ONLY_TABLE_ATTRIBUTE), and [`AppendOnly`](/core/javadoc/io/deephaven/engine/table/Table.html#APPEND_ONLY_TABLE_ATTRIBUTE) attributes. The example gets the value of its [`AddOnly`](/core/javadoc/io/deephaven/engine/table/Table.html#ADD_ONLY_TABLE_ATTRIBUTE) attribute.

```groovy test-set=2 order=:log
import io.deephaven.engine.table.impl.util.AppendOnlyArrayBackedInputTable

miamiInput = AppendOnlyArrayBackedInputTable.make(miami)
println miamiInput.getAttribute("AddOnly")
```

### `getAttributes`

The [`getAttributes`](../reference/table-operations/metadata/getAttributes.md) method returns _all_ the attributes in the source table's [`AttributeMap`](/core/javadoc/io/deephaven/engine/table/AttributeMap.html). To return only the attributes whose names match a predicate, use the overload that takes a `Predicate<String>`. The example uses the `miamiInput` input table from the [`getAttribute`](#getattribute) example.

```groovy test-set=2 order=:log
import java.util.function.Predicate

// include all attributes
println miamiInput.getAttributes()

// include only the "AddOnly" attribute
println miamiInput.getAttributes(Predicate.isEqual("AddOnly"))
```

### `getAttributeKeys`

[`getAttributeKeys`](../reference/table-operations/metadata/getAttributeKeys.md) returns an immutable set of the names of all the source table's attributes that have values. The example uses the `miamiInput` input table from the [`getAttribute`](#getattribute) example.

```groovy test-set=2 order=:log
println miamiInput.getAttributeKeys()
```

### `hasAttribute`

The [`hasAttribute`](../reference/table-operations/metadata/hasAttribute.md) method returns `true` if the source table has the specified attribute, and `false` otherwise. The example checks the `miamiInput` input table from the [`getAttribute`](#getattribute) example.

```groovy test-set=2 order=:log
println miamiInput.hasAttribute("AddOnly")
println miamiInput.hasAttribute("NotAnAttribute")
```

## Schema

The following methods describe a table's columns: their definitions, names, and number.

### `getDefinition`

Use the [`getDefinition`](../reference/table-operations/metadata/getDefinition.md) method to get the source table's [`TableDefinition`](/core/javadoc/io/deephaven/engine/table/TableDefinition.html).

```groovy test-set=2 order=:log
println miami.getDefinition()
```

### `getColumnNames`

For a list of a table's column names, call [`getColumnNames`](/core/javadoc/io/deephaven/engine/table/TableDefinition.html#getColumnNames()) on its [`TableDefinition`](/core/javadoc/io/deephaven/engine/table/TableDefinition.html):

```groovy test-set=2 order=:log
println miami.getDefinition().getColumnNames()
```

### `hasColumns`

The [`hasColumns`](../reference/table-operations/metadata/hasColumns.md) method returns `true` if the source table contains every column you name, or `false` if it lacks _any_ of them.

```groovy test-set=2 order=:log
println miami.hasColumns("Month")
println miami.hasColumns("Month", "NotAColumn")
```

### `numColumns`

The [`numColumns`](../reference/table-operations/metadata/numColumns.md) method returns the number of columns in the source table.

```groovy test-set=2 order=:log
println miami.numColumns()
```

## Table status

The following methods report a table's current state: whether it is a blink, empty, failed, flat, or refreshing table, how many rows it has, and which update graph it belongs to.

### `isBlink`

The [`isBlink`](../reference/table-operations/metadata/isBlink.md) method returns `true` if the source table is a [blink table](../conceptual/table-types.md#specialization-3-blink), and `false` otherwise. The static `miami` table is not a blink table. A [time table](./time-table.md#timetable-as-a-blink-table) built with `blinkTable(true)` is, because it keeps only the rows added in the current update cycle:

```groovy test-set=2 ticking-table order=:log
liveBlink = timeTableBuilder().period("PT1s").blinkTable(true).build()

println miami.isBlink()
println liveBlink.isBlink()
```

### `isEmpty`

The [`isEmpty`](../reference/table-operations/metadata/isEmpty.md) method returns `true` if the source table has no rows, and `false` otherwise.

```groovy test-set=2 order=:log
emptyTable0 = emptyTable(0)
println miami.isEmpty()
println emptyTable0.isEmpty()
```

### `isFailed`

The [`isFailed`](../reference/table-operations/metadata/isFailed.md) method returns `true` if the source table is in a failure state and no longer usable, and `false` otherwise.

```groovy test-set=2 order=:log
println miami.isFailed()
```

### `isFlat`

The [`isFlat`](../reference/table-operations/metadata/isFlat.md) method returns `true` if the source table is guaranteed to be flat, meaning its [row keys](../conceptual/table-update-model.md#describing-table-updates) run contiguously from 0 to the number of rows minus 1, and `false` otherwise.

```groovy test-set=2 order=:log
println miami.isFlat()
```

### `isRefreshing`

The [`isRefreshing`](../reference/table-operations/metadata/isRefreshing.md) method returns `true` if the source table is [refreshing](../conceptual/table-types.md#standard-streaming-tables), meaning its rows can change as new data arrives, and `false` otherwise. The `miami` table never changes, so it is not refreshing. A [time table](../reference/table-operations/create/timeTable.md) adds a row as time passes, so it is:

```groovy test-set=2 ticking-table order=:log
live = timeTable("PT1s")

println miami.isRefreshing()
println live.isRefreshing()
```

### `size`

The [`size`](/core/javadoc/io/deephaven/util/datastructures/LongSizedDataStructure.html#size()) method returns the number of rows in the source table.

```groovy test-set=2 order=:log
println miami.size()
```

### `getUpdateGraph`

To get the [UpdateGraph](../conceptual/dag.md#update-graph-cycles) that schedules a table's updates, use [`getUpdateGraph`](/core/javadoc/io/deephaven/engine/updategraph/NotificationQueue.Dependency.html#getUpdateGraph()):

```groovy test-set=2 order=:log
println miami.getUpdateGraph()
```

## `string`

Use the static [`string`](/core/javadoc/io/deephaven/engine/util/TableTools.html#string(io.deephaven.engine.table.Table,int,java.lang.String...)) method from [`TableTools`](/core/javadoc/io/deephaven/engine/util/TableTools.html) to return the first _n_ rows of a table as a pipe-delimited string. Pass the number of rows as the second argument; without it, `string` returns the first 10 rows. Any column names you pass after that limit the string to those columns.

```groovy test-set=2 order=:log
println string(miami, 4)
```

## `diff`

Use the static [`diff`](../reference/table-operations/metadata/diff.md) method from [`TableTools`](/core/javadoc/io/deephaven/engine/util/TableTools.html), which Groovy sessions import by default, to compare two tables and return the differences between them as a string. It returns an empty string if the tables are the same.

[`diff`](../reference/table-operations/metadata/diff.md) checks the tables in this order:

- Table size.
- Schema: missing or extra columns, column order, and column types.
- Data, column by column, for every column with the same name and type in both tables. It records only the first difference in each column.

It stops once it has recorded the number of differences you pass as its third argument.

To illustrate, let's compare two tables whose `B` and `C` columns differ on alternating rows.

```groovy order=:log
t1 = emptyTable(10).update("A = i", "B = i", "C = i")
t2 = emptyTable(10).update("A = i", "B = i % 2 == 0 ? i : i + 1", "C = i % 2 == 0 ? i + 1 : i")

// get the diff string
d = diff(t1, t2, 10).split("\n")

println d
```

The two tables have the same schema, so the only differences are in the data. [`diff`](../reference/table-operations/metadata/diff.md) treats the first table as the actual result and the second as the expected one, so each line reads "encountered _`t1` value_ expected _`t2` value_". It reports that column `B` first differs at row 1 and column `C` at row 0. Column `A` is identical in both tables.

## Related documentation

- [Create a new table](./new-and-empty-table.md#newtable)
- [How to perform dedicated aggregations for groups](./dedicated-aggregations.md)
- [Create and use input tables](./input-tables.md)
- [Create a time table](./time-table.md)
- [Deephaven's table types](../conceptual/table-types.md)
- [Deephaven's live Directed-Acyclic-Graph (DAG)](../conceptual/dag.md)
- [Supported Parquet formats](./data-import-export/parquet-formats.md)
- [`meta`](../reference/table-operations/metadata/meta.md)
- [`avgBy`](../reference/table-operations/group-and-aggregate/avgBy.md)
- [`diff`](../reference/table-operations/metadata/diff.md)
- [`getAttribute`](../reference/table-operations/metadata/getAttribute.md)
- [`getAttributes`](../reference/table-operations/metadata/getAttributes.md)
- [`getAttributeKeys`](../reference/table-operations/metadata/getAttributeKeys.md)
- [`getDefinition`](../reference/table-operations/metadata/getDefinition.md)
- [`hasAttribute`](../reference/table-operations/metadata/hasAttribute.md)
- [`hasColumns`](../reference/table-operations/metadata/hasColumns.md)
- [`InputTable`](../reference/table-operations/create/InputTable.md)
- [`isBlink`](../reference/table-operations/metadata/isBlink.md)
- [`isEmpty`](../reference/table-operations/metadata/isEmpty.md)
- [`isFailed`](../reference/table-operations/metadata/isFailed.md)
- [`isFlat`](../reference/table-operations/metadata/isFlat.md)
- [`isRefreshing`](../reference/table-operations/metadata/isRefreshing.md)
- [`numColumns`](../reference/table-operations/metadata/numColumns.md)
- [`timeTable`](../reference/table-operations/create/timeTable.md)
- [Javadoc](/core/javadoc/io/deephaven/engine/table/Table.html)
