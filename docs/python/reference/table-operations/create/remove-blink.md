---
title: remove_blink
---

The `remove_blink` method removes the blink table attribute from a table, disabling [specialized blink table aggregation semantics](../../conceptual/table-types.md#specialized-semantics-for-blink-tables). It returns the source table itself if it is already not a blink table.

> [!NOTE]
> `remove_blink` only removes the blink _attribute_ — **rows will still disappear each update cycle**. If you want rows to persist, use [`blink_to_append_only`](./blink-to-append-only.md) instead.

**What it does:**

- Disables [specialized aggregation semantics](../../conceptual/table-types.md#specialized-semantics-for-blink-tables) for blink tables. By default, aggregations like [`sum_by`](../group-and-aggregate/sumBy.md) on blink tables accumulate results over the _entire history_ of observed rows. After calling `remove_blink`, aggregations only operate on rows present in the current update cycle.

**What it does NOT do:**

- It does **not** make rows persist. The resulting table still exhibits the "blink" update pattern — all rows from the previous cycle are removed, and only new rows appear each cycle.
- It does **not** convert the table to an append-only or standard streaming table.

## Syntax

```python syntax
table.remove_blink() -> Table
```

## Parameters

This method takes no arguments.

## Returns

A table without the blink attribute. The table still removes all rows each update cycle.

## Example

The following example demonstrates the difference in aggregation behavior. With the blink attribute, `sum_by` accumulates over all rows ever seen. After `remove_blink`, `sum_by` only sums rows in the current cycle.

```python ticking-table order=t_blink_sum,t_no_blink_sum
from deephaven import time_table

t_blink = time_table("PT0.5s", blink_table=True).update(
    ["X = ii % 5", "Group = ii % 2 == 0 ? `A` : `B`"]
)

# With blink attribute: sum accumulates over ALL rows ever seen
t_blink_sum = t_blink.view(["X", "Group"]).sum_by("Group")

# After remove_blink: sum only includes rows in the CURRENT cycle
t_no_blink = t_blink.remove_blink()
t_no_blink_sum = t_no_blink.view(["X", "Group"]).sum_by("Group")
```

In this example:

- `t_blink_sum` continuously grows as it aggregates over all historical data.
- `t_no_blink_sum` only reflects the sum of rows present in the current update cycle (which is just one row per group per cycle in this case).

## Related documentation

- [blink_to_append_only](./blink-to-append-only.md) — Convert a blink table to append-only to preserve all rows.
- [Table types: Specialized semantics for blink tables](../../conceptual/table-types.md#specialized-semantics-for-blink-tables) — Detailed explanation of blink table aggregation behavior.
- [`time_table`](./timeTable.md)
