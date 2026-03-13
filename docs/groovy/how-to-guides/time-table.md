---
title: Create a time table
sidebar_label: Time tables
---

This guide will show you how to create a time table. A time table is a ticking, in-memory table that adds new rows at a regular, user-defined interval. Its sole column is a timestamp column.

Time tables are often used as trigger tables, which, through the use of [`snapshotWhen`](../reference/table-operations/snapshot/snapshot-when.md), can:

- reduce the update frequency of ticking tables.
- create the history of a table, sampled at a regular interval.

## timeTable

The [`timeTable`](../reference/table-operations/create/timeTable.md) method creates a table that ticks at the input period. The period can be passed in as nanoseconds:

```groovy ticking-table order=null
oneMinute = 60_000_000_000
result = timeTable(oneMinute)
```

Or as a [duration](../reference/query-language/types/durations.md) string:

```groovy ticking-table order=null
result = timeTable("PT2S")
```

In this example, the `startTime` argument was not provided, so the first row of the table will be _approximately_ the current time. See [this document](../reference/table-operations/create/timeTable.md#details-on-the-starttime-parameter) for more details.

> [!TIP]
> Duration strings are formatted as `"PTnHnMnS"`, where:
>
> - `PT` is the prefix to indicate a [duration](../reference/query-language/types/durations.md).
> - `n` is a number.
> - `H`, `M`, and `S` are the units of time (hours, minutes, and seconds, respectively).

<LoopedVideo src='../assets/tutorials/timetable.mp4' />

### timeTable with a start time

You can also pass a `startTime` to specify the timestamp of the first row in the time table:

```groovy ticking-table order=null
oneHourEarlier = minus(now(), parseDuration("PT1H"))

result = timeTable(oneHourEarlier, "PT2S").reverse()
```

When this code is run, `result` is initially populated with 1800 rows of data, one for every two seconds in the previous hour.

![`result` populates nearly instantly with 1800 rows of data](../assets/how-to/ticking-1h-earlier.gif)

### timeTable as a blink table

By default, the result of `timeTable` is [append-only](../conceptual/table-types.md#specialization-1-append-only). You can create a [blink](../conceptual/table-types.md#specialization-3-blink) table with the [`TimeTable.Builder`](/core/javadoc/io/deephaven/engine/table/impl/TimeTable.Builder.html) by calling the builder's `blinkTable` method. The resulting table only retains rows from the most recent update cycle:

```groovy ticking-table order=null
result = timeTableBuilder().period("PT2S").blinkTable(true).build()
```

<LoopedVideo src='../assets/how-to/groovy-tt-blink.mp4' />

## Related documentation

- [Create a new table](./new-and-empty-table.md#newtable)
- [How to capture the history of ticking tables](../how-to-guides/capture-table-history.md)
- [How to reduce the update frequency of ticking tables](../how-to-guides/performance/reduce-update-frequency.md)
- [Table types](../conceptual/table-types.md)
- [`snapshot`](../reference/table-operations/snapshot/snapshot.md)
- [`snapshotWhen`](../reference/table-operations/snapshot/snapshot-when.md)
- [`timeTable`](../reference/table-operations/create/timeTable.md)
