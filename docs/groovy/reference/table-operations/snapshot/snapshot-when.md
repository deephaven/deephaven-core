---
title: snapshotWhen
---

The `snapshotWhen` method produces an in-memory copy of a source table that adds a new snapshot when another table, the trigger table, changes.

> [!NOTE]
> The trigger table is often a [time table](../create/timeTable.md), a special type of table that adds new rows at a regular, user-defined interval.

> [!CAUTION]
> When `snapshotWhen` stores table history, it stores a copy of the source table for every trigger event. This means large source tables or rapidly changing trigger tables can result in large memory usage.

## Syntax

```
result = source.snapshotWhen(trigger)
result = source.snapshotWhen(trigger, features...)
result = source.snapshotWhen(trigger, features, stampColumns...)
result = source.snapshotWhen(trigger, options)
```

## Parameters

<ParamTable>
<Param name="trigger" type="Table">

The table that triggers the snapshot. This should be a ticking table, as changes in this table trigger the snapshot.

</Param>
<Param name="features" type="SnapshotWhenOptions.Flag...">

The snapshot features.

</Param>
<Param name="features" type="Collection<SnapshotWhenOptions.Flag>">

The snapshot features.

</Param>
<Param name="stampColumns" type="String...">

The stamp columns.

</Param>
<Param name="options" type="SnapshotWhenOptions">

The options dictate whether to take an initial snapshot, do incremental snapshots, keep history, and set stamp columns.

</Param>
</ParamTable>

> [!CAUTION]
> The trigger table's stamp column(s) appear in the result table. If the source table has a column with the same name as the stamp column, an error will be raised. To avoid this problem, rename the stamp column in the trigger table using [`renameColumns`](../select/rename-columns.md).

## Returns

An in-memory history of a source table that adds a new snapshot when the trigger table updates.

## SnapshotWhenOptions

The `snapshotWhen` method's `options` parameter takes a `SnapshotWhenOptions` object. These are constructed using `SnapshotWhenOptions.of(...)`. The syntax is given below.

```
import io.deephaven.api.snapshot.SnapshotWhenOptions

myOpts = SnapshotWhenOptions.of(INITIAL, INCREMENTAL, HISTORY, stampColumns)
```

Where `INITIAL`, `INCREMENTAL`, and `HISTORY` are booleans and `stampColumns` is one or more string values corresponding to columns in the `trigger` table.

- `INITIAL` (Boolean): Whether or not to take an initial snapshot upon creating the result table. The default is `false`.
- `INCREMENTAL` (Boolean): Determines whether the resulting table should be incremental. The default value is `false`. When `false`, the stamp column in the resulting table will always contain the _latest_ value from the stamp column in the trigger table. This means that every row in the resulting table will be updated each cycle. When `true`, only rows that have been added or updated to the source table since the last snapshot will have the latest value from the stamp column.
- `HISTORY` (Boolean): Determines whether the resulting table should keep history. The default value is `false`. When `true`, a full snapshot of the source table and the stamp column is appended to the resulting table every time the trigger table changes. This means that the resulting table will grow very fast. When `false`, only rows from the source table that have changed since the last snapshot will be appended to the resulting table.
- `stampColumns` (String...): One or more column names to act as stamp columns. Each stamp column will be included in the final result and will contain the value of the stamp column from the trigger table at the time of the snapshot. The default value is `None`, meaning all trigger table columns will be appended to the source table.

> [!NOTE]
> If `HISTORY` is `true`, `INITIAL` and `INCREMENTAL` _must_ be `false`.

## Examples

In the following example, the `source` table updates once every second. The `trigger` table updates once every five seconds. Thus, the `result` table updates once every five seconds. To avoid a name conflict error, the `Timestamp` column in the `trigger` is [renamed](/core/docs/reference/table-operations/select/rename-columns/).

```groovy ticking-table order=null
source = timeTable("PT00:00:01").updateView("X = i")
trigger = timeTable("PT00:00:05").renameColumns("TriggerTimestamp = Timestamp").updateView("Y = Math.sin(0.1 * i)")
result = source.snapshotWhen(trigger)
```

![The above `source`, `trigger`, and `result` tables](../../../assets/reference/table-operations/snapshot-when-1.gif)

Notice three things:

1. `stamp_cols` is left blank, so every column from `trigger` is included in `result`.
2. `incremental` is `false`, so the _entire_ `TriggerTimestamp` column in `result` is updated every cycle and always contains the latest value from the `TriggerTimestamp` column in `trigger`.
3. `historical` is `false`, so only _updated_ rows from `source` get appended to `result` on each snapshot.

In the following example, the code is nearly identical to the one above it. However, in this case, the `Y` column is given as the stamp key. Thus, the `Timestamp` column in the `trigger` table is omitted from the `result` table, which avoids a name conflict error. This is an alternative to renaming the column in the trigger table.

```groovy ticking-table order=null
import io.deephaven.api.snapshot.SnapshotWhenOptions

myOpts = SnapshotWhenOptions.of(false, false, false, "Y")

source = timeTable("PT00:00:01").updateView("X = i")
trigger = timeTable("PT00:00:05").updateView("Y = Math.sin(0.1 * i)")
result = source.snapshotWhen(trigger, myOpts)
```

![The above `source`, `trigger`, and `result` tables](../../../assets/reference/table-operations/snapshot-when-2.gif)

In the following example, `HISTORY` is set to `true`. Therefore, _every_ row in `source` gets snapshotted and appended to `result` when `trigger` changes, regardless of whether `source` has changed.

```groovy ticking-table order=null
import io.deephaven.api.snapshot.SnapshotWhenOptions

myOpts = SnapshotWhenOptions.of(false, false, true, "Y")

source = timeTable("PT00:00:01").updateView("X = i")
trigger = timeTable("PT00:00:05").updateView("Y = Math.sin(0.1 * i)")
result = source.snapshotWhen(trigger, myOpts)
```

![The above `source`, `trigger`, and `result` tables](../../../assets/reference/table-operations/snapshot-when-3.gif)

In the following example, `INCREMENTAL` is set to `true`. Thus, the `Y` column in `result` only updates when corresponding rows in `trigger` have changed. Contrast this with the first and second examples given above.

```groovy ticking-table order=null
import io.deephaven.api.snapshot.SnapshotWhenOptions

myOpts = SnapshotWhenOptions.of(false, true, false, "Y")

source = timeTable("PT00:00:01").updateView("X = i")
trigger = timeTable("PT00:00:05").updateView("Y = Math.sin(0.1 * i)")
result = source.snapshotWhen(trigger, myOpts)
```

![The above `source`, `trigger`, and `result` tables](../../../assets/reference/table-operations/snapshot-when-4.gif)

## Related documentation

- [Create a new table](../../../how-to-guides/new-and-empty-table.md#newtable)
- [Capture the history of a ticking table](../../../how-to-guides/capture-table-history.md)
- [How to reduce the update frequency of ticking tables](../../../how-to-guides/reduce-update-frequency.md)
- [`snapshot`](./snapshot.md)
- [`timeTable`](../create/timeTable.md)
- [`update`](../select/update.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/engine/table/impl/QueryTable.html#snapshotWhen(io.deephaven.engine.table.Table,io.deephaven.api.snapshot.SnapshotWhenOptions))
