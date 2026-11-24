---
title: timeTable
---

The `timeTable` method creates a ticking time table with one date-time column, `Timestamp`, whose values are spaced at a specified interval. The table adds rows during update graph cycles, as described in [Details on row timing](#details-on-row-timing).

## Syntax

```groovy syntax
timeTable(period)
timeTable(period, replayer)
timeTable(startTime, period)
timeTable(startTime, period, replayer)
timeTable(periodNanos)
timeTable(periodNanos, replayer)
timeTable(startTime, periodNanos)
timeTable(startTime, periodNanos, replayer)
timeTable(clock, startTime, periodNanos)
```

To create a [blink](../../../conceptual/table-types.md#specialization-3-blink) time table, or to combine a custom clock with a `String` period, use [`timeTableBuilder`](/core/javadoc/io/deephaven/engine/util/TableTools.html#timeTableBuilder()), which returns a [`TimeTable.Builder`](/core/javadoc/io/deephaven/engine/table/impl/TimeTable.Builder.html).

```groovy syntax
timeTableBuilder()
    .period(period)
    .startTime(startTime)
    .clock(clock)
    .blinkTable(blinkTable)
    .build()
```

## Parameters

<ParamTable>
<Param name="period" type="String">

The time interval between the timestamps of consecutive rows, as a [duration](../../query-language/types/durations.md) string, either in ISO-8601 format, such as `"PT1S"`, or in time format (`PThh:mm[:ss[.nnnnnnnnn]]`), such as `"PT00:00:01"`. Must be positive. The builder's `period` method also accepts a `Duration` or a `long` number of nanoseconds. The builder requires `period`.

</Param>
<Param name="periodNanos" type="long">

The time interval between the timestamps of consecutive rows, in nanoseconds. Must be positive.

</Param>
<Param name="startTime" type="Instant or String">

The value of the [date-time](../../query-language/types/date-time.md) in the first row of the table. The method parses `String` values with [`parseInstant`](../../time/datetime/parseInstant.md), for example `"2024-01-01T09:30:00 ET"`. The `clock` overload accepts only an `Instant`. See the [details on the `startTime` parameter](#details-on-the-starttime-parameter) for information on default behavior.

> [!WARNING]
> Setting `startTime` sufficiently far in the past, coupled with a short period, can cause the time table to instantly populate with a very large number of rows.

If the start time is in the future, the table stays empty until the time table's clock reaches it.

</Param>
<Param name="replayer" type="ReplayerInterface">

A data replayer whose clock determines the current time for the time table. If `null`, the table uses the current clock, which is the system clock unless a custom clock has been set. See [Replay data from static tables](../../../how-to-guides/replay-data.md).

</Param>
<Param name="clock" type="Clock">

The [`Clock`](/core/javadoc/io/deephaven/base/clock/Clock.html) that determines the current time for the time table. The `timeTable(clock, startTime, periodNanos)` overload requires it. With the builder, the table uses the current clock unless `clock` is called.

</Param>
<Param name="blinkTable" type="boolean">

Available only through [`timeTableBuilder`](/core/javadoc/io/deephaven/engine/util/TableTools.html#timeTableBuilder()). Whether the time table is a [blink table](../../../conceptual/table-types.md#specialization-3-blink). Defaults to `false`.

</Param>
</ParamTable>

## Returns

A ticking time table whose `Timestamp` values are spaced at the specified interval. The table is [append-only](../../../conceptual/table-types.md#specialization-1-append-only) unless it is built with `blinkTable(true)`, in which case it is a blink table.

## Examples

The following example creates a time table whose rows are one second apart. Since the example passes no `startTime` argument, the table starts [_approximately_](#details-on-the-starttime-parameter) at the current time.

```groovy ticking-table order=null
result = timeTable("PT1S")
```

<LoopedVideo src='../../../assets/reference/create/timeTable.mp4' />

The following example creates a time table with a start time two hours before its creation. The table immediately fills with one row per second from the start time up to the current time.

```groovy ticking-table order=null
startTime = minus(now(), parseDuration("PT2H"))

source = timeTable(startTime, "PT1S")
```

![The above time table](../../../assets/reference/create/timeTable2.gif)

The following example creates a [blink](../../../conceptual/table-types.md#specialization-3-blink) time table whose rows are one second apart. After each update cycle, the table holds only the rows added in that cycle.

```groovy ticking-table order=null
result = timeTableBuilder().period("PT1S").blinkTable(true).build()
```

The following example uses the `clock` overload, which requires a [`Clock`](/core/javadoc/io/deephaven/base/clock/Clock.html), an `Instant` start time, and a period in nanoseconds. It passes the current clock, so the result matches `timeTable(now(), 1_000_000_000L)`. Pass a different `Clock` to drive the table from another time source.

```groovy ticking-table order=null
resultClock = timeTable(currentClock(), now(), 1_000_000_000L)
```

## Details on the `startTime` parameter

When no `startTime` value is provided, the first entry in the time table is "now" according to the time table's clock, rounded down to the nearest multiple of the `period` based on the UTC epoch. The time table's clock is the replayer's clock when a `replayer` is passed, or the `clock` argument when one is passed. Otherwise, it is the current clock, which is the system clock unless a custom clock has been set.

Critically, this is _not necessarily_ the time that the function is initially called. Rather, it is the time of the first [update graph](../../../conceptual/dag.md#update-graph-cycles) (UG) cycle that runs after the [exclusive lock](../../../conceptual/query-engine/engine-locking.md#exclusive-lock) held during the initial call is released. Consider an example that first calls `timeTable` to create a ticking table, then does some other work that takes a significant amount of time. When this block of code runs all at once in the console, Deephaven holds the UG exclusive lock until the whole block completes.

```groovy test-set=1 ticking-table order=null
initialExecutionTime = now()
t = timeTable("PT1S")

// do some work
sleep(5000)
```

It is reasonable to expect the first row of the ticking table to be within one second of `initialExecutionTime`. However, this is not the case.

```groovy test-set=1
t = t.snapshot().update("InitialExecutionTime = initialExecutionTime")
```

The first row's timestamp is roughly five seconds later than the initial execution time of `timeTable`. This is because the "work" the script performs with `sleep` runs while the UG exclusive lock is held, and that lock is not released until the work is complete. The table cannot start ticking until the lock is released, which happens roughly five seconds after `timeTable` is initially called.

## Details on row timing

After creation, the table adds rows during [update graph](../../../conceptual/dag.md#update-graph-cycles) cycles, not on a separate timer. Each cycle adds every row whose timestamp has passed, so a period shorter than the cycle interval adds several rows per cycle, and a longer period leaves some cycles with no new rows. By default, the engine targets one update cycle per second.

## Related documentation

- [Create a table with `timeTable`](../../../how-to-guides/time-table.md)
- [How to reduce the update frequency of ticking tables](../../../how-to-guides/performance/reduce-update-frequency.md)
- [How to capture the history of ticking tables](../../../how-to-guides/capture-table-history.md)
- [Replay data from static tables](../../../how-to-guides/replay-data.md)
- [Deephaven's table types](../../../conceptual/table-types.md)
- [Deephaven's live directed acyclic graph (DAG)](../../../conceptual/dag.md)
- [Multithreading: Synchronization, locks, and snapshots](../../../conceptual/query-engine/engine-locking.md)
- [Durations](../../query-language/types/durations.md)
- [Date-time](../../query-language/types/date-time.md)
- [`emptyTable`](./emptyTable.md)
- [`newTable`](./newTable.md)
- [`now`](../../time/datetime/now.md)
- [`parseDuration`](../../time/datetime/parseDuration.md)
- [`parseInstant`](../../time/datetime/parseInstant.md)
- [Javadoc](/core/javadoc/io/deephaven/engine/util/TableTools.html#timeTable(java.lang.String))
