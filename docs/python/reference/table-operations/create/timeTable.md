---
title: time_table
---

The `time_table` method creates a time table that adds new rows at a specified interval. The resulting table has one date-time column, `Timestamp`.

## Syntax

```python syntax
time_table(
  period: Union[dtypes.Duration, int, str, datetime.timedelta, np.timedelta64, pd.Timdelta],
  start_time: Union[None, str, datetime.datetime, np.datetime64] = None,
  blink_table: bool = False,
  ) -> Table
```

## Parameters

<ParamTable>
<Param name="period" type="Union[dtypes.Duration, int, str, datetime.timedelta, np.timedelta64, pd.Timdelta]">

The time interval between new row additions. Can be given as:

- [Duration](../../query-language/types/durations.md) string such as `"PT1S"` or `"PT00:01:00"`.
- Integer format is in nanoseconds.
- String input format is "PT00:00:00:00.001" - days, hours, minutes, seconds, and milliseconds.
- A [datetime.timedelta](https://docs.python.org/3/library/datetime.html#datetime.timedelta) object.
- A [numpy.timedelta64](https://numpy.org/doc/stable/reference/arrays.scalars.html#numpy.timedelta64) object.
- A [pandas.Timedelta](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.Timedelta.html) object.

</Param>
<Param name="start_time" type="Union[None, str, datetime.datetime, np.datetime64" optional>

The value of the [date-time](../../query-language/types/date-time.md) in the first row of the table. String inputs must be given in syntax matching `YYYY-MM-DDThh:mm:ss.dddddddd TZ`. See the [details on the `start_time` parameter](#details-on-the-start_time-parameter) for information on default behavior.

> [!WARNING]
> Setting `start_time` sufficiently far in the past, coupled with a short `period`, can cause the time table to instantly populate with a very large number of rows.

</Param>
<Param name="blink_table" type="bool" optional>

If the time table should be a [blink table](../../../conceptual/table-types.md#specialization-3-blink); defaults to `False`.

</Param>
</ParamTable>

## Returns

A ticking time table that adds new rows at the specified interval.

## Example

The following example creates a time table that adds one new row every second. Since no `start_time` argument is provided, it starts [_approximately_](#details-on-the-start_time-parameter) at the current time.

```python ticking-table order=null
from deephaven import time_table

result = time_table("PT1S")
```

<LoopedVideo src='../../../assets/reference/create/timeTable.mp4' />

The following example creates a time table with a start time two hours before its creation. Upon creation, the entire table is populated from the start time up to the current time.

```python ticking-table order=null
from deephaven import time_table
import datetime as dt

starttime = dt.datetime.now() - dt.timedelta(hours=2)

t = time_table("PT1S", starttime)
```

![The above time table](../../../assets/reference/create/timeTable2.gif)

## Details on the `start_time` parameter

When no `start_time` value is provided, the first entry in the time table will be "now" according to the internal system clock, rounded down to the nearest multiple of the `period` based on the UTC epoch.

Critically, this is _not necessarily_ the time that the function is initially called. Rather, it is the moment when the UG lock for the initial call is released. Consider an example that first calls `time_table` to create a ticking table, then does some other work that takes a significant amount of time. By executing this block of code all at once, a UG lock is held until it completes.

```python test-set=1 order=null
from deephaven import time_table
from deephaven.time import dh_now
import time

initial_execution_time = dh_now()
t = time_table("PT1s")

# do some work
time.sleep(5)
```

It's reasonable to expect the first row of the ticking table to be within one second of `initial_execution_time`. However, this is not the case.

```python test-set=1
t = t.snapshot().update("InitialExecutionTime = initial_execution_time")
```

The first row of the ticking table is roughly 5 seconds ahead of the initial execution time of `time_table`. This is because the "work" the script performs with `time.sleep` holds a lock on the UG, and that lock is not released until the work is complete. The table cannot start ticking until the lock is released, which happens roughly five seconds after `time_table` is initially called.

## Related documentation

- [Create a table with `time_table`](../../../how-to-guides/time-table.md)
- [How to reduce the update frequency of ticking tables](../../../how-to-guides/performance/reduce-update-frequency.md)
- [How to capture the history of ticking tables](../../../how-to-guides/capture-table-history.md)
- [`empty_table`](./emptyTable.md)
- [`new_table`](./newTable.md)
- [`dh_now`](../../time/datetime/dh_now.md)
- [`to_j_duration`](../../time/datetime/to_j_duration.md)
- [Pydoc](/core/pydoc/code/deephaven.table_factory.html#deephaven.table_factory.time_table)
