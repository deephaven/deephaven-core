---
title: How to dial down the update frequency of ticking tables
---

This guide shows two ways to make the tables that depend on a [ticking table](../../conceptual/table-types.md#standard-streaming-tables) recompute less often. You can sample the table at a regular interval with [`snapshotWhen`](../../reference/table-operations/snapshot/snapshot-when.md), or take a [static](../../conceptual/table-types.md#static-tables) copy of it with [`snapshot`](../../reference/table-operations/snapshot/snapshot.md).

When a table updates, Deephaven must also update every downstream table — each table that depends on it for data. For fast-changing data, keeping them all up to date can use significant CPU time, which matters most on limited hardware.

Neither method slows down the source table. Each returns a new table. The [`snapshotWhen`](../../reference/table-operations/snapshot/snapshot-when.md) result updates only when a second table, called the trigger table, ticks. The [`snapshot`](../../reference/table-operations/snapshot/snapshot.md) result never updates. Build downstream queries on the returned table instead of on the source table, so they recompute only when the returned table changes.

## Sample at a regular interval

[`snapshotWhen`](../../reference/table-operations/snapshot/snapshot-when.md) copies a table (`source`) into an in-memory result each time another table (`trigger`) ticks. To sample a [ticking table](../../conceptual/table-types.md#standard-streaming-tables) at a fixed interval, pass a [time table](../time-table.md) as the trigger.

> [!NOTE]
> The trigger table is often a [time table](../time-table.md), a special type of table that adds new rows at a regular, user-defined interval. The sole column of a [time table](../time-table.md) is `Timestamp`.

In this example, the `source` table updates every second with new data. The `trigger` table updates every 5 seconds, and each tick takes a new snapshot of `source` into the `result` table. The example renames the `trigger` table's `Timestamp` column to `TriggerTimestamp` with [`renameColumns`](../../reference/table-operations/select/rename-columns.md) so that it doesn't conflict with the `Timestamp` column of `source`.

```groovy ticking-table order=null test-set=sample-interval
source = timeTable("PT1S").update("X = randomInt(0, 100)", "Y = sqrt(X)")

trigger = timeTable("PT5S").renameColumns("TriggerTimestamp = Timestamp")

result = source.snapshotWhen(trigger)
```

![The ticking `source`, `trigger`, and `result` tables above](../../assets/how-to/snapshot-when-basic.gif)

The `trigger` time table adds its first row right away, so the first snapshot appears almost immediately. After that, `result` and its downstream tables change once every 5 seconds instead of every second. This design pattern is useful for reducing the amount of data that must be processed.

Each snapshot replaces the entire result, so every update sends all rows of `source` downstream. To pass along only the rows that changed since the previous snapshot, pass the [`INCREMENTAL`](/core/javadoc/io/deephaven/api/snapshot/SnapshotWhenOptions.Flag.html#INCREMENTAL) flag. The flag needs `import io.deephaven.api.snapshot.SnapshotWhenOptions` at the top of your script:

```groovy ticking-table order=null test-set=sample-interval
import io.deephaven.api.snapshot.SnapshotWhenOptions

resultIncremental = source.snapshotWhen(trigger, SnapshotWhenOptions.Flag.INCREMENTAL)
```

Each trigger tick now passes along only the rows that `source` added since the previous snapshot, not every row. [Control what each snapshot does](#control-what-each-snapshot-does) covers this flag and the others.

### Control what each snapshot does

The [`SnapshotWhenOptions.Flag`](/core/javadoc/io/deephaven/api/snapshot/SnapshotWhenOptions.Flag.html) values and the `stampColumns` argument of [`snapshotWhen`](../../reference/table-operations/snapshot/snapshot-when.md) control how each snapshot changes the result and which trigger columns the result gets:

- [`INITIAL`](/core/javadoc/io/deephaven/api/snapshot/SnapshotWhenOptions.Flag.html#INITIAL): by default, the result stays empty until the trigger table first ticks. Pass `SnapshotWhenOptions.Flag.INITIAL` to take a snapshot as soon as the result is created.
- [`INCREMENTAL`](/core/javadoc/io/deephaven/api/snapshot/SnapshotWhenOptions.Flag.html#INCREMENTAL): by default, each snapshot replaces the entire result. Pass `SnapshotWhenOptions.Flag.INCREMENTAL` to apply only the rows of `source` that were added, modified, or removed since the previous snapshot, so downstream tables receive only those changes.
- [`HISTORY`](/core/javadoc/io/deephaven/api/snapshot/SnapshotWhenOptions.Flag.html#HISTORY): by default, each snapshot replaces the result. Pass `SnapshotWhenOptions.Flag.HISTORY` to append a full copy of `source` for each trigger-table row, including the rows the trigger table already has when you create the result. See [Capture the history of ticking tables](../capture-table-history.md).
  - The trigger table must be [append-only](../../conceptual/table-types.md#specialization-1-append-only). If it modifies or removes a row, the result fails and stops updating.
  - You can't combine `HISTORY` with `INITIAL` or `INCREMENTAL`.
- `stampColumns`: by default, the result gets every column of the trigger table. Pass `stampColumns` to choose which trigger columns to add. Each entry can rename the column, such as `"TriggerTimestamp = Timestamp"`.

Each snapshot fills the trigger columns with values from the trigger table's last row. With `INCREMENTAL`, only the added and modified rows get the new values. With `HISTORY`, each appended copy gets the values of the trigger row that produced it.

You can call `snapshotWhen` in four ways:

```groovy syntax
result = source.snapshotWhen(trigger)
result = source.snapshotWhen(trigger, features...)
result = source.snapshotWhen(trigger, features, stampColumns...)
result = source.snapshotWhen(trigger, options)
```

The last three forms differ in how they take the flags and stamp columns:

- [`snapshotWhen(trigger, features...)`](../../reference/table-operations/snapshot/snapshot-when.md): one or more [`SnapshotWhenOptions.Flag`](/core/javadoc/io/deephaven/api/snapshot/SnapshotWhenOptions.Flag.html) values.
- [`snapshotWhen(trigger, features, stampColumns...)`](../../reference/table-operations/snapshot/snapshot-when.md): a collection of flags, such as `[SnapshotWhenOptions.Flag.INCREMENTAL]`, followed by the stamp columns.
- [`snapshotWhen(trigger, options)`](../../reference/table-operations/snapshot/snapshot-when.md): a [`SnapshotWhenOptions`](../../reference/table-operations/snapshot/snapshot-when.md#snapshotwhenoptions) object that holds both the flags and the stamp columns. Create it with [`SnapshotWhenOptions.of`](../../reference/table-operations/snapshot/snapshot-when.md#snapshotwhenoptions).

The following calls use the `source` and renamed `trigger` tables from the example above. They import [`SnapshotWhenOptions`](../../reference/table-operations/snapshot/snapshot-when.md#snapshotwhenoptions) and its `Flag` enum, then pass concrete flags and stamp columns:

```groovy syntax
import io.deephaven.api.snapshot.SnapshotWhenOptions
import io.deephaven.api.snapshot.SnapshotWhenOptions.Flag

result = source.snapshotWhen(trigger, Flag.INITIAL)
result = source.snapshotWhen(trigger, Flag.INCREMENTAL)
result = source.snapshotWhen(trigger, Flag.HISTORY)
result = source.snapshotWhen(trigger, [Flag.INCREMENTAL], "SnapshotTime = TriggerTimestamp")
result = source.snapshotWhen(trigger, SnapshotWhenOptions.of(true, true, false, "SnapshotTime = TriggerTimestamp"))
```

> [!CAUTION]
> The result gets every trigger-table column by default, so a column name that both tables share throws an exception. To avoid this, rename the column in one table with [`renameColumns`](../../reference/table-operations/select/rename-columns.md), or use `stampColumns` to leave out or rename the trigger column.

## Take a static copy

When downstream tables need the data only as of one moment, such as for a one-off report, take a static copy. Creating a static snapshot of a ticking table is as easy as calling [`snapshot`](../../reference/table-operations/snapshot/snapshot.md) on the table. It returns a [static](../../conceptual/table-types.md#static-tables) copy of the table as of the moment you call it. The copy, and any table built only from it, never updates.

This example creates a [ticking table](../../conceptual/table-types.md#standard-streaming-tables) and, after it has ticked for a while, calls [`snapshot`](../../reference/table-operations/snapshot/snapshot.md) to capture its rows at that moment. First, create the ticking table:

```groovy ticking-table order=null test-set=static-snapshot
source = timeTable("PT0.1S").updateView("X = 0.1 * i", "Y = sin(X)")
```

After the table has ticked for some time, call [`snapshot`](../../reference/table-operations/snapshot/snapshot.md). The `result` table holds the rows of `source` at that moment and never updates:

```groovy ticking-table order=null test-set=static-snapshot
result = source.snapshot()
```

![The ticking `source` and static `result`](../../assets/how-to/snapshot-static.gif)

## Related documentation

- [Table types](../../conceptual/table-types.md)
- [Create a time table](../time-table.md)
- [Capture the history of ticking tables](../capture-table-history.md)
- [`renameColumns`](../../reference/table-operations/select/rename-columns.md)
- [`snapshot`](../../reference/table-operations/snapshot/snapshot.md)
- [`snapshotWhen`](../../reference/table-operations/snapshot/snapshot-when.md)
- [`timeTable`](../../reference/table-operations/create/timeTable.md)
- [`update`](../../reference/table-operations/select/update.md)
- [`updateView`](../../reference/table-operations/select/update-view.md)
- [`SnapshotWhenOptions` Javadoc](/core/javadoc/io/deephaven/api/snapshot/SnapshotWhenOptions.html)
- [`TableOperations` Javadoc](/core/javadoc/io/deephaven/api/TableOperations.html)
