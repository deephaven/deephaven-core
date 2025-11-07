---
title: Create a time table
sidebar_label: Time tables
---

This guide will show you how to create a time table. A time table is a ticking, in-memory table that adds new rows at a regular, user-defined interval. Its sole column is a timestamp column.

Time tables are often used as trigger tables, which, through the use of [`snapshot_when`](../reference/table-operations/snapshot/snapshot-when.md), can:

- reduce the update frequency of ticking tables.
- create the history of a table, sampled at a regular interval.

## time_table

The [`time_table`](../reference/table-operations/create/timeTable.md) method creates a table that ticks at the input period. The period can be passed in as nanoseconds:

```python ticking-table order=null
from deephaven import time_table

minute = 1_000_000_000 * 60
result = time_table(period=minute)
```

Or as a [duration](../reference/query-language/types/durations.md) string:

```python ticking-table order=null
from deephaven import time_table

result = time_table(period="PT2S")
```

In this example, the `start_time` argument was not provided, so the first row of the table will be _approximately_ the current time. See [this document](../reference/table-operations/create/timeTable.md#details-on-the-start_time-parameter) for more details.

> [!TIP]
> Duration strings are formatted as `"PTnHnMnS"`, where:
>
> - `PT` is the prefix to indicate a [duration](../reference/query-language/types/durations.md).
> - `n` is a number.
> - `H`, `M`, and `S` are the units of time (hours, minutes, and seconds, respectively).

<LoopedVideo src='../assets/tutorials/timetable.mp4' />

### time_table with a start time

You can also pass a `start_time` to specify the timestamp of the first row in the time table:

```python ticking-table order=null
from deephaven import time_table
import datetime

one_hour_earlier = datetime.datetime.now() - datetime.timedelta(hours=1)

result = time_table(period="PT2S", start_time=one_hour_earlier).reverse()
```

When this code is run, `result` is initially populated with 1800 rows of data, one for every two seconds in the previous hour.

![`result` populates nearly instantly with 1800 rows of data](../assets/how-to/ticking-1h-earlier.gif)

### time_table as a blink table

By default, the result of time_table is [append-only](../conceptual/table-types.md#specialization-1-append-only). You can set the `blink_table` parameter to `True` to create a [blink](../conceptual/table-types.md#specialization-3-blink) table, which only retains rows from the most recent update cycle:

```python ticking-table order=null
from deephaven import time_table

result = time_table("PT00:00:02", blink_table=True)
```

<LoopedVideo src='../assets/how-to/blink_time_table.mp4' />

## Related documentation

- [Create a new table](./new-and-empty-table.md#new_table)
- [How to capture the history of ticking tables](../how-to-guides/capture-table-history.md)
- [How to reduce the update frequency of ticking tables](../how-to-guides/performance/reduce-update-frequency.md)
- [Table types](../conceptual/table-types.md)
- [`snapshot`](../reference/table-operations/snapshot/snapshot.md)
- [`snapshot_when`](../reference/table-operations/snapshot/snapshot-when.md)
- [`time_table`](../reference/table-operations/create/timeTable.md)
