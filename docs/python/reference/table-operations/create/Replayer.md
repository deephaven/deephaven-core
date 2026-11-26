---
title: TableReplayer
---

`TableReplayer` replays historical data with timestamps as a new [ticking table](../../../conceptual/table-types.md).

The table that `add_table` returns starts with every row whose timestamp is at or before `start_time`. When [`start`](/core/pydoc/code/deephaven.replay.html#deephaven.replay.TableReplayer.start) runs, the replay clock begins at `start_time` and advances in real time, and the replayer adds each remaining row when the replay clock reaches that row's timestamp. The clock stops at `end_time`, so rows with later timestamps are never added, and the replayer then shuts down. All tables registered with the same replayer share one replay clock, so their rows appear on the same timeline. Each table must have a timestamp column that meets the [timestamp requirements](#timestamp-requirements).

## Syntax

```python syntax
TableReplayer(start_time: InstantLike, end_time: InstantLike) -> TableReplayer
```

## Parameters

<ParamTable>
<Param name="start_time" type="InstantLike">

The time at which the replay clock starts. Can be an `Instant`, `int` (nanoseconds since the Epoch), `str`, `datetime.datetime`, `numpy.datetime64`, or `pandas.Timestamp`.

</Param>
<Param name="end_time" type="InstantLike">

The time at which the replay stops. Can be an `Instant`, `int` (nanoseconds since the Epoch), `str`, `datetime.datetime`, `numpy.datetime64`, or `pandas.Timestamp`.

</Param>
</ParamTable>

## Returns

A `TableReplayer` object that replays historical data.

## Methods

`TableReplayer` supports the following methods:

- [`add_table`](/core/pydoc/code/deephaven.replay.html#deephaven.replay.TableReplayer.add_table) - Registers a table for replaying and returns the replayed ticking table.
- [`start`](/core/pydoc/code/deephaven.replay.html#deephaven.replay.TableReplayer.start) - Starts replaying data.
- [`shutdown`](/core/pydoc/code/deephaven.replay.html#deephaven.replay.TableReplayer.shutdown) - Shuts down and permanently invalidates the replayer.

`TableReplayer` wraps a Java [`Replayer`](/core/javadoc/io/deephaven/engine/table/impl/replay/Replayer.html), available through its `j_object` property. The grouped replay methods, [`replayGrouped`](/core/javadoc/io/deephaven/engine/table/impl/replay/Replayer.html#replayGrouped(io.deephaven.engine.table.Table,java.lang.String,java.lang.String)) and [`replayGroupedLastBy`](/core/javadoc/io/deephaven/engine/table/impl/replay/Replayer.html#replayGroupedLastBy(io.deephaven.engine.table.Table,java.lang.String,java.lang.String...)), are reachable only through that Java object.

## Timestamp requirements

Each table registered with a replayer through [`add_table`](/core/pydoc/code/deephaven.replay.html#deephaven.replay.TableReplayer.add_table) must have a timestamp column of type [`Instant`](../../query-language/types/date-time.md). The column must not contain null values, and it must be sorted, so that its values do not decrease from one row to the next. Sort the table on that column with [`sort`](../sort/sort.md) first if needed. The replayer checks each timestamp one row ahead, so it reports a null or decreasing value early: either `add_table` raises an error, or the replayed table fails when the replay clock reaches the timestamp of the row before the bad one. If the table is refreshing, the replayer replays a snapshot taken when the table is registered.

For the grouped replay methods, `replayGrouped` and `replayGroupedLastBy`, the timestamp column must contain no null values and must be sorted within each group.

> [!NOTE]
> Grouped replay does not check the timestamps. In the current release, a row whose timestamp is earlier than `start_time`, or a null timestamp at the head of a group, makes the replay fail with an error, such as an engine assertion failure or a `NullPointerException`, depending on the data. With `replayGrouped`, a later null or a value that decreases within its group is added in the same update cycle as the row before it, out of time order, and the table does not fail. With `replayGroupedLastBy`, such a row is skipped and never appears, and when several rows of one group become due in the same update cycle, the table can show one of the earlier rows rather than the latest.

## Examples

The following example creates a table of historical data with timestamps and then replays it.

```python order=result,replayed_result ticking-table
from deephaven import new_table
from deephaven.column import datetime_col, int_col
from deephaven.replay import TableReplayer
from deephaven.time import to_j_instant

result = new_table(
    [
        datetime_col(
            "DateTime",
            [
                to_j_instant("2000-01-01T00:00:01 ET"),
                to_j_instant("2000-01-01T00:00:03 ET"),
                to_j_instant("2000-01-01T00:00:06 ET"),
            ],
        ),
        int_col("Number", [1, 3, 6]),
    ]
)

start_time = to_j_instant("2000-01-01T00:00:00 ET")
end_time = to_j_instant("2000-01-01T00:00:07 ET")

result_replayer = TableReplayer(start_time, end_time)

replayed_result = result_replayer.add_table(result, "DateTime")

result_replayer.start()
```

The following example replays two tables with the same replayer, so both update on the same replay clock.

```python order=source_1,source_2,replayed_source_1,replayed_source_2 ticking-table
from deephaven import empty_table
from deephaven.replay import TableReplayer

source_1 = empty_table(20).update(["Timestamp = '2024-01-01T08:00:00 ET' + i * SECOND"])
source_2 = empty_table(25).update(
    ["Timestamp = '2024-01-01T08:00:00 ET' + i * (long)(0.8 * SECOND)"]
)

replayer = TableReplayer(
    start_time="2024-01-01T08:00:00 ET", end_time="2024-01-01T08:00:20 ET"
)

replayed_source_1 = replayer.add_table(table=source_1, col="Timestamp")
replayed_source_2 = replayer.add_table(table=source_2, col="Timestamp")
replayer.start()
```

## Related documentation

- [How to replay historical data](../../../how-to-guides/replay-data.md)
- [Table types](../../../conceptual/table-types.md)
- [Date-time types](../../query-language/types/date-time.md)
- [`sort`](../sort/sort.md)
- [Javadoc](/core/javadoc/io/deephaven/engine/table/impl/replay/Replayer.html)
- [Pydoc](/core/pydoc/code/deephaven.replay.html#deephaven.replay.TableReplayer)
