---
title: How to dial down the update frequency of ticking tables
sidebar_label: Dial down the update frequency of ticking tables
---

This guide will show you how to reduce the update frequency of ticking tables.

When a table updates, all of the children of the table - which depend on the source table for data - must also be updated. For fast-changing data, this can mean a lot of computing to keep child tables up to date. Table snapshots allow the update frequency of a table to be reduced, which results in fewer updates of child tables. This can be useful when processing fast-changing data on limited hardware.

## `snapshot_when` and `snapshot`

The [`snapshot_when`](../../reference/table-operations/snapshot/snapshot-when.md) operation produces an in-memory copy of a table (`source`), which refreshes every time another table (`trigger`) ticks.

```python syntax
result = source.snapshot_when(trigger)
result = source.snapshot_when(trigger_table=trigger, stamp_cols=stamp_keys)
result = source.snapshot_when(trigger_table=trigger, initial=True)
result = source.snapshot_when(trigger_table=trigger, incremental=True)
result = source.snapshot_when(trigger_table=trigger, history=True)
```

> [!NOTE]
> The trigger table is often a [time table](../../reference/table-operations/create/timeTable.md), a special type of table that adds new rows at a regular, user-defined interval. The sole column of a time table is `Timestamp`.

> [!CAUTION]
> Columns from the trigger table appear in the result table. If the trigger and source tables have columns with the same name, an error will be raised. To avoid this, either rename conflicting columns, or omit duplicates in the trigger table via stamp columns.

The [`snapshot`](../../reference/table-operations/snapshot/snapshot.md) operation produces a static snapshot of a table at a specific point in time.

```python syntax
result = source.snapshot() -> Table
```

## Sample at a regular interval

In this example, the `source` table updates every second with new data. The `trigger` table updates every 5 seconds, triggering a new snapshot of the `source` table (`result`). This design pattern is useful for reducing the amount of data that must be processed.

```python ticking-table order=null
from deephaven import time_table
import random

source = time_table("PT1S").update(
    formulas=["X = (int)random.randint(0, 100)", "Y = sqrt(X)"]
)

trigger = time_table("PT5S").rename_columns(cols=["TriggerTimestamp = Timestamp"])

result = source.snapshot_when(trigger_table=trigger)
```

![The ticking `source`, `trigger`, and `result` tables above](../../assets/how-to/snapshot-when-basic.gif)

## Create a static snapshot

Creating a static snapshot of a ticking table is as easy as calling [`snapshot`](../../reference/table-operations/snapshot/snapshot.md) on the table.

This example creates a ticking table, and then after some time, calls [`snapshot`](../../reference/table-operations/snapshot/snapshot.md) to capture a moment in the table's history.

```python ticking-table order=null
from deephaven import time_table

source = time_table("PT0.1S").update_view(["X = 0.1 * i", "Y = Math.sin(X)"])

# After some time
result = source.snapshot()
```

![The ticking `source` and static `result`](../../assets/how-to/snapshot-static.gif)

## Related documentation

- [Create a time table](../time-table.md)
- [Capture the history of ticking tables](../capture-table-history.md)
- [`empty_table`](../../reference/table-operations/create/emptyTable.md)
- [`rename_columns`](../../reference/table-operations/select/rename-columns.md)
- [`snapshot`](../../reference/table-operations/snapshot/snapshot.md)
- [`snapshot_when`](../../reference/table-operations/snapshot/snapshot-when.md)
- [`time_table`](../../reference/table-operations/create/timeTable.md)
- [`update`](../../reference/table-operations/select/update.md)
