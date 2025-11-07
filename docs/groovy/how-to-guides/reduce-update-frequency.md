---
title: Dial down the update frequency of ticking tables
---

This guide will show you how to reduce the update frequency of ticking tables.

When a table updates, all of the children of the table - which depend on the source table for data - must also be updated. For fast-changing data, this can mean a lot of computing to keep child tables up to date. Table snapshots allow the update frequency of a table to be reduced, which results in fewer updates of child tables. This can be useful when processing fast-changing data on limited hardware.

## `snapshotWhen` and `snapshot`

The [`snapshotWhen`](../reference/table-operations/snapshot/snapshot-when.md) operation produces an in-memory copy of a table (`source`), which refreshes every time another table (`trigger`) ticks.

```groovy syntax
result = source.snapshotWhen(trigger)
result = source.snapshotWhen(trigger, features...)
result = source.snapshotWhen(trigger, features, stampColumns...)
result = source.snapshotWhen(trigger, options)
```

> [!NOTE]
> The trigger table is often a [time table](../reference/table-operations/create/timeTable.md), a special type of table that adds new rows at a regular, user-defined interval. The sole column of a time table is `Timestamp`.

> [!CAUTION]
> Columns from the trigger table appear in the result table. If the trigger and source tables have any columns of the same name, an error will occur. To avoid this, either rename conflicting columns, or omit duplicates in the trigger table via stamp columns.

The [`snapshot`](../reference/table-operations/snapshot/snapshot.md) operation produces a static snapshot of a table at a specific point in time.

```groovy syntax
result = source.snapshot()
```

## Sample at a regular interval

In this example, the `source` table updates every 0.5 seconds with new data. The `trigger` table updates every 5 seconds, triggering a new snapshot of the `source` table (`result`). This design pattern is useful for reducing the amount of data that must be processed.

```groovy ticking-table order=null
source = timeTable("PT00:00:00.5").update("X = new Random().nextInt(100)", "Y = sqrt(X)")

trigger = timeTable("PT00:00:05").renameColumns("TriggerTimestamp = Timestamp")

result = source.snapshotWhen(trigger)
```

## Create a static snapshot

Creating a static snapshot of a ticking table is as easy as calling [`snapshot`](../reference/table-operations/snapshot/snapshot.md) on the table.

This example creates a ticking table, and then after some time, calls [`snapshot`](../reference/table-operations/snapshot/snapshot.md) to capture a moment in the table's history.

```groovy ticking-table order=null
source = timeTable("PT00:00:01").updateView("X = 0.1 * i", "Y = Math.sin(X)")

// After some time
result = source.snapshot()
```

## Related documentation

- [Create a time table](./time-table.md)
- [Capture the history of ticking tables](./capture-table-history.md)
- [`emptyTable`](../reference/table-operations/create/emptyTable.md)
- [`renameColumns`](../reference/table-operations/select/rename-columns.md)
- [`snapshot`](../reference/table-operations/snapshot/snapshot.md)
- [`snapshotWhen`](../reference/table-operations/snapshot/snapshot-when.md)
- [`timeTable`](../reference/table-operations/create/timeTable.md)
- [`update`](../reference/table-operations/select/update.md)
