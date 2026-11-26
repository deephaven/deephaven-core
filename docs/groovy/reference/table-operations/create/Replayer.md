---
title: Replayer
---

`Replayer` replays historical data with timestamps as a new [ticking table](../../../conceptual/table-types.md).

The table that `replay` returns starts with every row whose timestamp is at or before `startTime`. When [`start`](/core/javadoc/io/deephaven/engine/table/impl/replay/Replayer.html#start()) runs, the replay clock begins at `startTime` and advances in real time, and the replayer adds each remaining row when the replay clock reaches that row's timestamp. The clock stops at `endTime`, so rows with later timestamps are never added, and the replayer then shuts down. All tables registered with the same replayer share one replay clock, so their rows appear on the same timeline. Each table must have a timestamp column that meets the [timestamp requirements](#timestamp-requirements).

## Syntax

```groovy syntax
new Replayer(startTime, endTime)
```

## Parameters

<ParamTable>
<Param name="startTime" type="Instant">

The time at which the replay clock starts.

</Param>
<Param name="endTime" type="Instant">

The time at which the replay stops.

</Param>
</ParamTable>

## Returns

A `Replayer` object that replays historical data.

## Methods

`Replayer` supports the following methods:

- [`replay`](/core/javadoc/io/deephaven/engine/table/impl/replay/Replayer.html#replay(io.deephaven.engine.table.Table,java.lang.String)) - Registers a historical table and its timestamp column for replaying, and returns the replayed ticking table.
- [`replayGrouped`](/core/javadoc/io/deephaven/engine/table/impl/replay/Replayer.html#replayGrouped(io.deephaven.engine.table.Table,java.lang.String,java.lang.String)) - Returns a replayed version of a historical table, grouped by one column. It can be faster than [`replay`](/core/javadoc/io/deephaven/engine/table/impl/replay/Replayer.html#replay(io.deephaven.engine.table.Table,java.lang.String)). Within each group, rows are added in their original order; ordering between groups is not guaranteed.
- [`replayGroupedLastBy`](/core/javadoc/io/deephaven/engine/table/impl/replay/Replayer.html#replayGroupedLastBy(io.deephaven.engine.table.Table,java.lang.String,java.lang.String...)) - Returns a replayed last-by table that holds the latest row for each key in the given grouping columns, subject to the caveats in [Timestamp requirements](#timestamp-requirements).
- [`start`](/core/javadoc/io/deephaven/engine/table/impl/replay/Replayer.html#start()) - Starts replaying data.
- [`isDone`](/core/javadoc/io/deephaven/engine/table/impl/replay/Replayer.html#isDone()) - Returns whether the replayer has shut down, which happens after the replay clock reaches `endTime`, after a call to [`shutdown`](/core/javadoc/io/deephaven/engine/table/impl/replay/Replayer.html#shutdown()), or after a refresh error.
- [`waitDone`](/core/javadoc/io/deephaven/engine/table/impl/replay/Replayer.html#waitDone(long)) - Waits up to a specified number of milliseconds for the replayer to finish.
- [`shutdown`](/core/javadoc/io/deephaven/engine/table/impl/replay/Replayer.html#shutdown()) - Shuts the replayer down. The replayed tables stop updating, and the replayer cannot be used again.

`Replayer` also has lower-level public methods, such as [`setTime`](/core/javadoc/io/deephaven/engine/table/impl/replay/Replayer.html#setTime(long)), [`schedule`](/core/javadoc/io/deephaven/engine/table/impl/replay/Replayer.html#schedule(java.util.TimerTask,long,long)), and [`clock`](/core/javadoc/io/deephaven/engine/table/impl/replay/Replayer.html#clock()). The [Javadoc](/core/javadoc/io/deephaven/engine/table/impl/replay/Replayer.html) describes them.

## Timestamp requirements

Each table registered with a replayer through [`replay`](/core/javadoc/io/deephaven/engine/table/impl/replay/Replayer.html#replay(io.deephaven.engine.table.Table,java.lang.String)), [`replayGrouped`](/core/javadoc/io/deephaven/engine/table/impl/replay/Replayer.html#replayGrouped(io.deephaven.engine.table.Table,java.lang.String,java.lang.String)), or [`replayGroupedLastBy`](/core/javadoc/io/deephaven/engine/table/impl/replay/Replayer.html#replayGroupedLastBy(io.deephaven.engine.table.Table,java.lang.String,java.lang.String...)) must have a timestamp column of type [`Instant`](../../query-language/types/date-time.md) that contains no null values. The column must be sorted: for `replay`, its values must not decrease from one row to the next, and for `replayGrouped` and `replayGroupedLastBy`, they must not decrease within each group. Sort the table on that column with [`sort`](../sort/sort.md) first if needed. `replay` checks each timestamp one row ahead, so it reports a null or decreasing value early: either `replay` throws an exception, or the replayed table fails when the replay clock reaches the timestamp of the row before the bad one. If the table is refreshing, the replayer replays a snapshot taken when the table is registered.

> [!NOTE]
> `replayGrouped` and `replayGroupedLastBy` do not check the timestamps. In the current release, a row whose timestamp is earlier than `startTime`, or a null timestamp at the head of a group, makes the replay fail with an error, such as an engine assertion failure or a `NullPointerException`, depending on the data. With `replayGrouped`, a later null or a value that decreases within its group is added in the same update cycle as the row before it, out of time order, and the table does not fail. With `replayGroupedLastBy`, such a row is skipped and never appears, and when several rows of one group become due in the same update cycle, the table can show one of the earlier rows rather than the latest.

## Examples

The following example creates a table of historical data with timestamps and then replays it.

```groovy order=result,replayedResult ticking-table
import io.deephaven.engine.table.impl.replay.Replayer

result = newTable(
    instantCol("DateTime", parseInstant("2000-01-01T00:00:01 ET"), parseInstant("2000-01-01T00:00:03 ET"), parseInstant("2000-01-01T00:00:06 ET")),
    intCol("Number", 1, 3, 6)
)

startTime = parseInstant("2000-01-01T00:00:00 ET")
endTime = parseInstant("2000-01-01T00:00:07 ET")

resultReplayer = new Replayer(startTime, endTime)

replayedResult = resultReplayer.replay(result, "DateTime")

resultReplayer.start()
```

<LoopedVideo src='../../../assets/reference/create/Replayer_refExample.mp4' />

The following example replays two tables with the same replayer, so both update on the same replay clock.

```groovy order=source1,source2,replayedSource1,replayedSource2 ticking-table
import io.deephaven.engine.table.impl.replay.Replayer

source1 = emptyTable(20).update("Timestamp = '2024-01-01T08:00:00 ET' + i * SECOND")
source2 = emptyTable(25).update("Timestamp = '2024-01-01T08:00:00 ET' + i * (long)(0.8 * SECOND)")

replayer = new Replayer(parseInstant("2024-01-01T08:00:00 ET"), parseInstant("2024-01-01T08:00:20 ET"))

replayedSource1 = replayer.replay(source1, "Timestamp")
replayedSource2 = replayer.replay(source2, "Timestamp")
replayer.start()
```

## Related documentation

- [How to replay historical data](../../../how-to-guides/replay-data.md)
- [Table types](../../../conceptual/table-types.md)
- [Date-time types](../../query-language/types/date-time.md)
- [`sort`](../sort/sort.md)
- [Javadoc](/core/javadoc/io/deephaven/engine/table/impl/replay/Replayer.html)
