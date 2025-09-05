---
title: Create static tables
sidebar_label: Static tables
---

Deephaven excels as an interface for ingesting data, parsing it as a table, and manipulating the data. However, Deephaven also includes a range of versatile methods for creating tables from scratch. This guide will show you how to create tables in Deephaven using the following methods:

- [`newTable`](#newtable)
- [`emptyTable`](#emptytable)
- [`RingTableTools.of`](#ringtabletoolsof)
- [`timeTable`](#timetable)
- [`Replayer`](#replay-historical-data)

We also discuss creating input tables with `AppendOnlyArrayBackedInputTable.make` and `KeyedArrayBackedInputTable.make`.

## `newTable`

The most direct way to create a table in Deephaven is with the [`newTable`](../reference/table-operations/create/newTable.md) method. This method initializes a table, and columns are placed into the table through one or more column methods such as [`intCol`](../reference/table-operations/create/intCol.md). Each column contains one data type. For example, [`intCol`](../reference/table-operations/create/intCol.md) creates a column of [Java primitive](https://docs.oracle.com/javase/tutorial/java/nutsandbolts/datatypes.html) `int` values.

The following query creates a new in-memory table with a `string` column and an `int` column.

```groovy
result = newTable(
    stringCol("NameOfStringCol", "Data String 1", "Data String 2", "Data String 3"),
    intCol("NameOfIntCol", 4, 5, 6),
)
```

This produces a table with a String column, an integer column, and three rows.

For a more in-depth look at creating tables with `newTable`, see the [`newTable`](./new-and-empty-table.md#newtable) guide.

## `emptyTable`

Unlike a table created with [`newTable`](../reference/table-operations/create/newTable.md), an empty table does not contain columns by default. The [`emptyTable`](../reference/table-operations/create/emptyTable.md) function takes a single argument - an `int` representing the number of rows in the new table. The resulting table has no columns and the specified number of rows:

```groovy order=table
table = emptyTable(10)
```

Calling [`emptyTable`](../reference/table-operations/create/emptyTable.md) on its own generates a table with no data, but it can easily be populated with columns and data using [`update`](../reference/table-operations/select/update.md) or another [selection method](./use-select-view-update.md). This can be done in the same line that creates the table, or at any time afterward.

In the following example, we create a table with 10 rows and a single column `X` with values 0 through 9 by using the [special variable `i`](../reference/query-language/variables/special-variables.md) to represent the row index. Then, the table is updated again to add a column `Y` with values equal to `X` squared:

```groovy order=table
table = emptyTable(10).update("X = i")

table = table.update("Y = X * X")
```

For a more in-depth look at creating tables with `emptyTable`, see the [`emptyTable`](./new-and-empty-table.md#emptytable) guide.

## `RingTableTools.of`

The `RingTableTools.of` method allows you to create a [ring table](../conceptual/table-types.md#specialization-4-ring) from a [blink table](../conceptual/table-types.md#specialization-3-blink) or an [append-only table](../conceptual/table-types.md#specialization-1-append-only).

In this example, we'll create a ring table with a three-row capacity from a simple append-only time table.

```groovy ticking-table order=null
import io.deephaven.engine.table.impl.sources.ring.RingTableTools

source = timeTable("PT00:00:01")
result = RingTableTools.of(source, 3)
```

![The above `source` and `result` tables ticking side-by-side](../assets/how-to/ring-table-1.gif)

For a more in-depth look at creating tables with `RingTableTools.of`, see the [`ring table`](./ring-table.md) guide.

## `timeTable`

A time table is a ticking, in-memory table that adds new rows at a regular, user-defined interval. Its sole column is a timestamp column.

The [`timeTable`](../reference/table-operations/create/timeTable.md) method creates a table that ticks at the input period. The period can be passed in as nanoseconds:

```groovy ticking-table order=null
minute = 1_000_000_000 * 60L
result = timeTable(minute)
```

Or as a [duration](../reference/query-language/types/durations.md) string:

```groovy ticking-table order=null
result = timeTable("PT2S")
```

<LoopedVideo src='../assets/tutorials/timetable.mp4' />

For an in-depth look at creating tables with `timeTable`, see the [`timeTable`](./time-table.md) guide.

## Input Tables

Input tables allow users to enter new data into tables in two ways: programmatically and manually through the UI.

In the first case, data is added to a table with `add`, an input table-specific method similar to [`merge`](/core/docs/reference/table-operations/merge/merge). In the second case, data is added to a table through the UI by clicking on cells and typing in the contents, similar to a spreadsheet program like [MS Excel](https://www.microsoft.com/en-us/microsoft-365/excel).

Input tables can be:

- [Append-only](../conceptual/table-types.md#specialization-1-append-only) - any entered data is added to the bottom of the table.
- [Keyed](#create-a-keyed-input-table) - contents can be modified or deleted; allows access to rows by key.
- In this guide, we'll create some simple append-only input tables. For a full guide to [`input_table`](../reference/table-operations/create/InputTable.md), see the [`input_table`](./input-tables.md) guide.

Here, we'll create an input table from a pre-existing table:

```groovy test-set-1 order=source,result
import io.deephaven.engine.table.impl.util.AppendOnlyArrayBackedInputTable

source = emptyTable(10).update("X = i")

result = AppendOnlyArrayBackedInputTable.make(source)
```

For a more in-depth look at creating input tables, see the [input table guide](./input-tables.md).

## Replay historical data

Deephaven's `Replayer` allows you to replay historical data as if it were live data. This is useful for experimenting with live tables and other purposes. In this guide, we'll show you how to replay historical data as real-time data based on timestamps in a table.

### Get a historical data table

To replay historical data, we need a table with timestamps in [`DateTime`](../reference/query-language/types/date-time.md) format. Let's grab one from Deephaven's [examples](https://github.com/deephaven/examples/) repository. We'll use data from a 100 km bike ride in a file called `metriccentury.csv`.

```groovy test-set=1 order=null
import io.deephaven.csv.CsvTools

metricCentury = CsvTools.readCsv(
    "https://media.githubusercontent.com/media/deephaven/examples/main/MetricCentury/csv/metriccentury.csv"
)
```

### Replay the data

The data is in memory. We can replay it with the following steps:

- Import [`Replayer`](../reference/table-operations/create/Replayer.md).
- Set a start and end time for data replay. These times correspond to those in the table itself.
- Create the replayer using the start and end time.
- Call [`replay`](../reference/table-operations/create/Replayer.md#methods) to prepare the replayed table.
  - This takes two inputs: the table and the `DateTime` column name.
- Call [`start`](../reference/table-operations/create/Replayer.md#methods) to start replaying data.

```groovy test-set=1 order=null ticking-table
import io.deephaven.engine.table.impl.replay.Replayer

start = parseInstant("2019-08-25T15:34:55Z")
end = parseInstant("2019-08-25T17:10:22Z")

myReplayer = new Replayer(start, end)
replayedTable = myReplayer.replay(metricCentury, "Time")
myReplayer.start()
```

The data will now replay in real time.

For a more in-depth look at replaying historical data, see the guide on [replaying data](./replay-data.md).

## Related documentation

- [`newTable`](../reference/table-operations/create/newTable.md)
- [`emptyTable`](../reference/table-operations/create/emptyTable.md)
- [`timeTable`](../reference/table-operations/create/timeTable.md)
- [`InputTable`](../reference/table-operations/create/InputTable.md)
- [`Replayer`](../reference/table-operations/create/Replayer.md)
