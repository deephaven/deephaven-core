---
title: Capture table history with snapshots
sidebar_label: Snapshot
---

This guide will show you how to capture the history of ticking tables.

Append-only tables are simple. New rows are added at the bottom of the table, and rows are never deleted or modified. This makes examining the history of append-only tables very easy. If a table is not append-only, rows are added, deleted, and modified, which makes examining the history more complex. By using [`snapshotWhen`](../reference/table-operations/snapshot/snapshot-when.md), you can capture the history of a table, even if it is not append-only.

## Syntax

[`snapshotWhen`](../reference/table-operations/snapshot/snapshot-when.md) produces an in-memory table that can contain the history of the source table. Values are added to the history every time the trigger table ticks.

```groovy syntax
result = source.snapshotWhen(trigger)
result = source.snapshotWhen(trigger, features...)
result = source.snapshotWhen(trigger, features, stampColumns...)
result = source.snapshotWhen(trigger, options)
```

> [!NOTE]
> The trigger table is often a [time table](../reference/table-operations/create/timeTable.md), a special type of table that adds new rows at a regular, user-defined interval.

> [!CAUTION]
> The stamp column(s) from the trigger table appears in the result table. If the source table has a column with the same name as the stamp column, an error will be raised. To avoid this problem, rename the stamp column in the trigger table using [`renameColumns`](../reference/table-operations/select/rename-columns.md).

> [!CAUTION]
> When using [`snapshot_when`](../reference/table-operations/snapshot/snapshot.md) to capture full table history, a copy of the entire source table is stored for every trigger event. Large source tables or rapidly changing trigger tables can result in intensive memory usage.

## Include a history

In this example, there are two input tables. The `source` table updates every 0.01 seconds with new data. The `trigger` table updates every second, triggering a new snapshot of the `source` table to be added to the `result` table. This design pattern is useful for examining the history of a table.

```groovy ticking-table order=null
import io.deephaven.api.snapshot.SnapshotWhenOptions

myOpts = SnapshotWhenOptions.of(false, false, true)

source = timeTable("PT00:00:00.1").update("X = i%2 == 0 ? `A` : `B`", "Y = new Random().nextInt(100)", "Z = sqrt(Y)").lastBy("X")
trigger = timeTable("PT00:00:02").renameColumns("TriggerTimestamp = Timestamp")
result = source.snapshotWhen(trigger, myOpts)
```

<LoopedVideo src='../assets/how-to/snapshot3.mp4' />

## Related documentation

- [Create a time table](../how-to-guides/time-table.md)
- [Reduce the update frequency of ticking tables](./performance/reduce-update-frequency.md)
- [`renameColumn`](../reference/table-operations/select/rename-columns.md)
- [`snapshot`](../reference/table-operations/snapshot/snapshot.md)
- [`snapshotWhen`](../reference/table-operations/snapshot/snapshot-when.md)
- [`timeTable`](../reference/table-operations/create/timeTable.md)
- [`update`](../reference/table-operations/select/update.md)
