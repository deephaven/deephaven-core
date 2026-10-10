---
title: Create a time table
---

This guide shows you how to create a time table. A time table is a [ticking](../conceptual/table-types.md), in-memory table that adds new rows at a regular, user-defined interval. Its sole column is a timestamp column named `Timestamp`.

Time tables often serve as trigger tables. A trigger table is a table whose updates cause [`snapshotWhen`](../reference/table-operations/snapshot/snapshot-when.md) to take a new snapshot of another table. Used this way, a time table can:

- [reduce the update frequency](./performance/reduce-update-frequency.md) of ticking tables
- [create the history of a table](./capture-table-history.md), sampled at a regular interval

## Set the period

The [`timeTable`](../reference/table-operations/create/timeTable.md) method creates a table that adds one row every period, the interval between rows. You can pass the period as a number of nanoseconds:

```groovy ticking-table order=null
oneMinute = 60_000_000_000
result = timeTable(oneMinute)
```

Or as a [duration](../reference/query-language/types/durations.md) string:

```groovy ticking-table order=null
result = timeTable("PT2S")
```

> [!TIP]
> Duration strings use the ISO-8601 format `"PnDTnHnMnS"`, where:
>
> - `P` is the prefix to indicate a [duration](../reference/query-language/types/durations.md).
> - `T` separates the day component from the time components.
> - `n` is a number.
> - `D`, `H`, `M`, and `S` are the units of time (days, hours, minutes, and seconds, respectively).
>
> You can leave out units with a value of zero, and seconds can have a fractional part. For example, `"PT2S"`, `"PT1H30M"`, and `"PT0.5S"` are all valid. Deephaven also accepts the clock-style form `"PThh:mm:ss"`, such as `"PT00:00:02"`.

<LoopedVideo src='../assets/tutorials/timetable.mp4' />

## Set the start time

If you don't pass a start time, the first row's timestamp is the time of the table's first [update cycle](../conceptual/table-update-model.md) (one pass in which the engine processes new data), rounded down to the nearest multiple of the period. That timestamp can be later than the moment you call [`timeTable`](../reference/table-operations/create/timeTable.md). See [Details on the `startTime` parameter](../reference/table-operations/create/timeTable.md#details-on-the-starttime-parameter) for more.

To specify the timestamp of the first row, pass a start time as the first argument:

```groovy ticking-table order=null
oneHourEarlier = minus(now(), parseDuration("PT1H"))

result = timeTable(oneHourEarlier, "PT2S").reverse()
```

When you run this code, `result` starts with at least 1801 rows: one at the start time and one for every two seconds after it, up to the current time. A delay of two seconds or more between computing the start time and creating the table adds one row for each full two-second period of delay.

The example calls [`reverse`](../reference/table-operations/sort/reverse.md) only so that the newest rows appear at the top. You don't need `reverse` to set a start time.

![`result` populates nearly instantly with an hour of data](../assets/how-to/ticking-1h-earlier.gif)

## Create a blink time table

By default, the result of [`timeTable`](../reference/table-operations/create/timeTable.md) is [append-only](../conceptual/table-types.md#specialization-1-append-only). A [blink](../conceptual/table-types.md#specialization-3-blink) table instead retains only the rows from the most recent [update cycle](../conceptual/table-update-model.md).

To create a blink time table, call [`timeTableBuilder`](/core/javadoc/io/deephaven/engine/util/TableTools.html#timeTableBuilder()), which returns a [`TimeTable.Builder`](/core/javadoc/io/deephaven/engine/table/impl/TimeTable.Builder.html). Then:

1. Set the period with [`period`](/core/javadoc/io/deephaven/engine/table/impl/TimeTable.Builder.html#period(java.lang.String)).
2. Pass `true` to [`blinkTable`](/core/javadoc/io/deephaven/engine/table/impl/TimeTable.Builder.html#blinkTable(boolean)).
3. Call [`build`](/core/javadoc/io/deephaven/engine/table/impl/TimeTable.Builder.html#build()).

In the following example, a new row arrives every two seconds. By default, the engine runs an update cycle about once per second, so only about every other cycle adds a row. After a cycle that adds a row, the table holds only the rows added in that cycle, usually one. A slow cycle can add more than one. After a cycle that adds no row, the table is empty:

```groovy ticking-table order=null
result = timeTableBuilder().period("PT2S").blinkTable(true).build()
```

<LoopedVideo src='../assets/how-to/groovy-tt-blink.mp4' />

## Related documentation

- [Create a new table](./new-and-empty-table.md#newtable)
- [How to capture the history of ticking tables](./capture-table-history.md)
- [How to reduce the update frequency of ticking tables](./performance/reduce-update-frequency.md)
- [Table types](../conceptual/table-types.md)
- [`snapshot`](../reference/table-operations/snapshot/snapshot.md)
- [`snapshotWhen`](../reference/table-operations/snapshot/snapshot-when.md)
- [`timeTable`](../reference/table-operations/create/timeTable.md)
