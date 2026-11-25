---
title: time_table
---

The `time_table` function creates a ticking time table with one date-time column, `Timestamp`, whose values are spaced at a specified interval. The table adds rows during update graph cycles, as described in [Details on row timing](#details-on-row-timing).

## Syntax

```python syntax
time_table(
  period: dtypes.Duration | int | str | datetime.timedelta | np.timedelta64 | pd.Timedelta,
  start_time: InstantLike | None = None,
  blink_table: bool = False,
  ) -> Table
```

## Parameters

<ParamTable>
<Param name="period" type="dtypes.Duration | int | str | datetime.timedelta | np.timedelta64 | pd.Timedelta">

The time interval between the timestamps of consecutive rows. Must be positive. Accepts any of the following:

- A [duration](../../query-language/types/durations.md) string, either in ISO-8601 format, such as `"PT1S"`, or in time format (`PThh:mm[:ss[.nnnnnnnnn]]`), such as `"PT00:01:00"` or `"PT00:00:00.001"`.
- An integer number of nanoseconds.
- A Java `Duration` (`dtypes.Duration`).
- A [datetime.timedelta](https://docs.python.org/3/library/datetime.html#datetime.timedelta) object.
- A [numpy.timedelta64](https://numpy.org/doc/stable/reference/arrays.scalars.html#numpy.timedelta64) object.
- A [pandas.Timedelta](https://pandas.pydata.org/docs/reference/api/pandas.Timedelta.html) object.

The function converts every accepted type with [`to_j_duration`](../../time/datetime/to_j_duration.md).

</Param>
<Param name="start_time" type="InstantLike | None" optional>

The value of the [date-time](../../query-language/types/date-time.md) in the first row of the table. Accepts an `Instant`, an `int` (nanoseconds since the epoch), a `str`, a `datetime.datetime`, a `numpy.datetime64`, or a `pandas.Timestamp`. The function converts the value with [`to_j_instant`](../../time/datetime/to_j_instant.md).

A string can use any format that [`DateTimeUtils.parseInstant`](/core/javadoc/io/deephaven/time/DateTimeUtils.html#parseInstant(java.lang.String)) accepts. Two examples are `yyyy-MM-ddThh:mm:ss[.SSSSSSSSS] TZ`, such as `"2024-01-01T09:30:00 ET"`, and a string of digits that gives seconds, milliseconds, microseconds, or nanoseconds since the epoch.

See the [details on the `start_time` parameter](#details-on-the-start_time-parameter) for information on default behavior.

> [!WARNING]
> Setting `start_time` sufficiently far in the past, coupled with a short `period`, can cause the time table to instantly populate with a very large number of rows.

If the start time is in the future, the table stays empty until the current clock reaches it.

</Param>
<Param name="blink_table" type="bool" optional>

Whether the time table is a [blink table](../../../conceptual/table-types.md#specialization-3-blink). Defaults to `False`.

</Param>
</ParamTable>

## Returns

A ticking time table whose `Timestamp` values are spaced at the specified interval. The table is [append-only](../../../conceptual/table-types.md#specialization-1-append-only) unless `blink_table` is `True`, in which case it is a blink table.

## Examples

The following example creates a time table whose rows are one second apart. Since the example passes no `start_time` argument, the table starts [_approximately_](#details-on-the-start_time-parameter) at the current time.

```python ticking-table order=null
from deephaven import time_table

result = time_table("PT1S")
```

<LoopedVideo src='../../../assets/reference/create/timeTable.mp4' />

The following example creates a time table with a start time two hours before its creation. The table immediately fills with one row per second from the start time up to the current time.

```python ticking-table order=null
from deephaven import time_table
import datetime as dt

start_time = dt.datetime.now() - dt.timedelta(hours=2)

source = time_table("PT1S", start_time)
```

![The above time table](../../../assets/reference/create/timeTable2.gif)

The following example creates a [blink](../../../conceptual/table-types.md#specialization-3-blink) time table whose rows are one second apart. After each update cycle, the table holds only the rows added in that cycle.

```python ticking-table order=null
from deephaven import time_table

result = time_table("PT1S", blink_table=True)
```

## Details on the `start_time` parameter

When no `start_time` value is provided, the first entry in the time table is "now" according to the current clock, rounded down to the nearest multiple of the `period` based on the UTC epoch. The current clock is the system clock unless a custom clock has been set.

Critically, this is _not necessarily_ the time that the function is initially called. Rather, it is the time of the first [update graph](../../../conceptual/dag.md#update-graph-cycles) (UG) cycle that runs after the [exclusive lock](../../../conceptual/query-engine/engine-locking.md#exclusive-lock) held during the initial call is released. Consider an example that first calls `time_table` to create a ticking table, then does some other work that takes a significant amount of time. When this block of code runs all at once in the console, Deephaven holds the UG exclusive lock until the whole block completes.

```python test-set=1 ticking-table order=null
from deephaven import time_table
from deephaven.time import dh_now
import time

initial_execution_time = dh_now()
t = time_table("PT1S")

# do some work
time.sleep(5)
```

It is reasonable to expect the first row of the ticking table to be within one second of `initial_execution_time`. However, this is not the case.

```python test-set=1
t = t.snapshot().update("InitialExecutionTime = initial_execution_time")
```

The first row's timestamp is roughly five seconds later than the initial execution time of `time_table`. This is because the "work" the script performs with `time.sleep` runs while the UG exclusive lock is held, and that lock is not released until the work is complete. The table cannot start ticking until the lock is released, which happens roughly five seconds after `time_table` is initially called.

## Details on row timing

After creation, the table adds rows during [update graph](../../../conceptual/dag.md#update-graph-cycles) cycles, not on a separate timer. Each cycle adds every row whose timestamp has passed, so a period shorter than the cycle interval adds several rows per cycle, and a longer period leaves some cycles with no new rows. By default, the engine targets one update cycle per second.

## Related documentation

- [Create a table with `time_table`](../../../how-to-guides/time-table.md)
- [How to reduce the update frequency of ticking tables](../../../how-to-guides/performance/reduce-update-frequency.md)
- [How to capture the history of ticking tables](../../../how-to-guides/capture-table-history.md)
- [Deephaven's table types](../../../conceptual/table-types.md)
- [Deephaven's live directed acyclic graph (DAG)](../../../conceptual/dag.md)
- [Multithreading: Synchronization, locks, and snapshots](../../../conceptual/query-engine/engine-locking.md)
- [Durations](../../query-language/types/durations.md)
- [Date-time](../../query-language/types/date-time.md)
- [`empty_table`](./emptyTable.md)
- [`new_table`](./newTable.md)
- [`dh_now`](../../time/datetime/dh_now.md)
- [`to_j_duration`](../../time/datetime/to_j_duration.md)
- [`to_j_instant`](../../time/datetime/to_j_instant.md)
- [Pydoc](/core/pydoc/code/deephaven.table_factory.html#deephaven.table_factory.time_table)
