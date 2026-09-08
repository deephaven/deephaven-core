---
title: snapshot_when
---

The `snapshot_when` method produces an in-memory copy of a source table that adds a new snapshot when another table, the trigger table, changes.

> [!NOTE]
> The trigger table is often a [time table](../create/timeTable.md), a special type of table that adds new rows at a regular, user-defined interval.

> [!CAUTION]
> When `snapshot_when` stores table history, it stores a copy of the source table for every trigger event. This means large source tables or rapidly changing trigger tables can result in large memory usage.

## Syntax

```
source.snapshot_when(
    trigger_table: Union[Table, PartitionedTableProxy],
    stamp_cols: list[str],
    initial: bool = False,
    incremental: bool = False,
    history: bool = False,
) -> PartitionedTableProxy
```

## Parameters

<ParamTable>
<Param name="trigger_table" type="Union[Table, PartitionedTableProxy]">

The table that triggers the snapshot. This should be a ticking table, as changes in this table trigger the snapshot.

</Param>
<Param name="stamp_cols" type="list[str]" optional>

One or more column names to act as stamp columns. Each stamp column will be included in the final result, and will contain the value of the stamp column from the trigger table at the time of the snapshot. If only one column, a string or list can be used. If more than one column, a list must be used. The default value is `None`, which means that all columns from the trigger table will be appended to the source table.

</Param>
<Param name="initial" type="bool" optional>

Determines whether an initial snapshot is taken upon construction. The default value is `False`

</Param>
<Param name="incremental" type="bool" optional>

Determines whether the resulting table should be incremental. The default value is `False`. When `False`, the stamp column in the resulting table will always contain the _latest_ value from the stamp column in the trigger table. This means that every single row in the resulting table will be updated each cycle. When `True`, only rows that have been added or updated to the source table since the last snapshot will have the latest value from the stamp column.

</Param>
<Param name="history" type="bool" optional>

Determines whether the resulting table should keep history. The default value is `False`. When `True`, a full snapshot of the source table and the stamp column is appended to the resulting table every time the trigger table changes. This means that the resulting table will grow very fast. When `False`, only rows from the source table that have changed since the last snapshot will be appended to the resulting table. If this is `True`, `incremental` and `initial` must be `False`.

</Param>
</ParamTable>

> [!CAUTION]
> The stamp column(s) from the trigger table appears in the result table. If the source table has a column with the same name as the stamp column, an error will be raised. To avoid this problem, rename the stamp column in the trigger table using [`rename_columns`](../select/rename-columns.md).

## Returns

A new table that captures a snapshot of the source table whenever the trigger table updates.

## Examples

In the following example, the `source` table updates once every second. The `trigger` table updates once every five seconds. Thus, the `result` table updates once every five seconds. The `Timestamp` column in the `trigger` is [renamed](../select/rename-columns.md) to avoid a name conflict error.

```python ticking-table order=null
from deephaven import time_table

source = time_table("PT1S").update_view(["X = i"])
trigger = (
    time_table("PT5S")
    .rename_columns(["TriggerTimestamp = Timestamp"])
    .update_view(["Y = Math.sin(0.1 * i)"])
)
result = source.snapshot_when(trigger_table=trigger)
```

![The above `source`, `trigger`, and `result` tables](../../../assets/reference/table-operations/snapshot-when-1.gif)

Notice three things:

1. `stamp_cols` is left blank, so every column from `trigger` is included in `result`.
2. `incremental` is `false`, so the _entire_ `TriggerTimestamp` column in `result` is updated every cycle and always contains the latest value from the `TriggerTimestamp` column in `trigger`.
3. `historical` is `false`, so only _updated_ rows from `source` get appended to `result` on each snapshot.

In the following example, the code is nearly identical to the one above it. However, in this case, the `Y` column is given as the stamp key. Thus, the `Timestamp` column in the `trigger` table is omitted from the `result` table, which avoids a name conflict error. This is an alternative to renaming the column in the trigger table.

```python ticking-table order=null
from deephaven import time_table

source = time_table("PT1S").update_view(["X = i"])
trigger = time_table("PT5S").update_view(["Y = i"])
result = source.snapshot_when(trigger_table=trigger, stamp_cols=["Y"])
```

![The above `source`, `trigger`, and `result` tables](../../../assets/reference/table-operations/snapshot-when-2.gif)

In the following example, `history` is set to `True`. Therefore, _every_ row in `source` gets snapshotted and appended to `result` when `trigger` changes, regardless of whether `source` has changed or not.

```python ticking-table order=null
from deephaven import time_table

source = time_table("PT1S").update_view(["X = i"])
trigger = time_table("PT5S").update_view(["Y = i"])
result = source.snapshot_when(trigger_table=trigger, history=True)
```

![The above `source`, `trigger`, and `result` tables](../../../assets/reference/table-operations/snapshot-when-3.gif)

In the following example, `incremental` is set to `True`. Thus, the `Y` column in `result` only updates when corresponding rows in `trigger` have changed. Contrast this with the first and second examples given above.

```python ticking-table order=null
from deephaven import time_table

source = time_table("PT1S").update_view(["X = i"])
trigger = time_table("PT5S").update_view(["Y = i"])
result = source.snapshot_when(trigger_table=trigger, stamp_cols=["Y"], incremental=True)
```

![The above `source`, `trigger`, and `result` tables](../../../assets/reference/table-operations/snapshot-when-4.gif)

## Related documentation

- [Create a new table](../../../how-to-guides/new-and-empty-table.md#new_table)
- [Capture the history of a ticking table](../../../how-to-guides/capture-table-history.md)
- [How to reduce the update frequency of ticking tables](../../../how-to-guides/performance/reduce-update-frequency.md)
- [`time_table`](../create/timeTable.md)
- [`update`](../select/update.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/engine/table/impl/QueryTable.html#snapshotWhen(io.deephaven.engine.table.Table,io.deephaven.api.snapshot.SnapshotWhenOptions))
- [Pydoc](/core/pydoc/code/deephaven.table.html#deephaven.table.Table.snapshot_when)
