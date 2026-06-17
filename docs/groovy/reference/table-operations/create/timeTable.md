---
title: timeTable
---

The `timeTable` method creates a time table that adds new rows at a specified interval. The resulting table has one date-time column, `Timestamp`.

## Syntax

```
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

## Parameters

<ParamTable>
<Param name="period" type="String">

The time interval between new row additions.

</Param>
<Param name="replayer" type="ReplayerInterface">

Data replayer.

</Param>
<Param name="startTime" type="DateTime">

Start time for adding new rows. See the [details on the `startTime` parameter](#details-on-the-starttime-parameter) for information on default behavior.

</Param>
<Param name="periodNanos" type="long">

The time interval between new rows in nanoseconds.

</Param>
<Param name="clock" type="Clock">

The clock.

</Param>
</ParamTable>

## Returns

A ticking time table that adds new rows at the specified interval.

## Example

The following example creates a time table that adds one new row every second.

```groovy ticking-table order=null
result = timeTable("PT00:00:01")
```

<LoopedVideo src='../../../assets/reference/create/timeTable.mp4' />

The following example creates a time table that starts two hours prior to its creation.

```groovy ticking-table order=null
startTime = minus(now(), parseDuration("PT2H"))

source = timeTable(startTime, "PT1S")
```

<!--TODO: add more code examples. Given the large number of overloads, that would be helpful. Esp. the one using a clock. -->

![The above time table](../../../assets/reference/create/timeTable2.gif)

## Details on the `startTime` parameter

When no `startTime` value is provided, the first entry in the time table will be "now" according to the internal system clock, rounded down to the nearest multiple of the `period` based on the UTC epoch.

Critically, this is _not necessarily_ the time that the function is initially called. Rather, it is the moment when the UG lock for the initial call is released. Consider an example that first calls `timeTable` to create a ticking table, then does some other work that takes a significant amount of time. By executing this block of code all at once, a UG lock is held until it completes.

```groovy test-set=1 ticking-table order=null
initial_execution_time = now()
t = timeTable("PT1s")

// do some work
sleep(5000)
```

It's reasonable to expect the first row of the ticking table to be within one second of `initial_execution_time`. However, this is not the case.

```groovy test-set=1
t = t.snapshot().update("InitialExecutionTime = initial_execution_time")
```

The first row of the ticking table is roughly 5 seconds ahead of the initial execution time of `timeTable`. This is because the "work" the script performs with `sleep` holds a lock on the UG, and that lock is not released until the work is complete. The table cannot start ticking until the lock is released, which happens roughly five seconds after `timeTable` is initially called.

## Related documentation

- [Create a table with `timeTable`](../../../how-to-guides/time-table.md)
- [How to reduce the update frequency of ticking tables](../../../how-to-guides/performance/reduce-update-frequency.md)
- [How to capture the history of ticking tables](../../../how-to-guides/capture-table-history.md)
- [`emptyTable`](./emptyTable.md)
- [`newTable`](./newTable.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/engine/util/TableTools.html#timeTable(java.lang.String))
