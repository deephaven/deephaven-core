---
title: remove_blink
---

The `remove_blink` method removes the blink attribute from a [blink table](../../../conceptual/table-types.md#specialization-3-blink). The blink attribute is a marker on the table that tells Deephaven to apply blink table semantics, including [specialized aggregation semantics](../../../conceptual/table-types.md#specialized-semantics-for-blink-tables). Removing it turns those semantics off, so later operations on the result treat it as an ordinary refreshing table. [Effects](#effects) lists what changes.

> [!NOTE]
> `remove_blink` only removes the blink _attribute_ — **rows still disappear each update cycle**. Each cycle removes all rows from the previous cycle, and only new rows appear, so the result is not an append-only table. To keep rows across update cycles, use [`blink_to_append_only`](./blink-to-append-only.md) instead.

## Syntax

```python syntax
table.remove_blink() -> Table
```

## Parameters

This method takes no arguments.

## Returns

A table without the blink attribute. If the source table is not a blink table, the returned `Table` object wraps the same underlying engine table as the source.

## Effects

Operations on the result behave as follows:

- Aggregations such as [`sum_by`](../group-and-aggregate/sumBy.md) operate only on the rows in the current [update cycle](../../../conceptual/table-update-model.md) instead of on every row the blink table has delivered since the aggregation was created. The same applies to other operations that treat blink tables specially, such as [`select_distinct`](../select/select-distinct.md), [`head`](../filter/head.md), and [`tail`](../filter/tail.md). When a blink table is the filter table for [`where_in`](../filter/where-in.md) or [`where_not_in`](../filter/where-not-in.md), removing the attribute makes the filter use only the current cycle's keys.
- Operations that raise an error on a blink table, such as [`group_by`](../group-and-aggregate/groupBy.md) and [`partition_by`](../group-and-aggregate/partitionBy.md), run on the result and operate only on the rows of the current update cycle.
- Formulas that use the [special variables](../../query-language/variables/special-variables.md) `i`, `ii`, or `k`, or column array expressions other than a simple constant offset such as `X_[i-1]`, raise an error when applied to the result, because the result is no longer marked as holding only the current cycle's rows. Apply such formulas before calling `remove_blink`, as the [example](#example) does.

See [Unsupported operations](../../../conceptual/table-types.md#unsupported-operations) for the full list of operations that a blink table does not support, and [Partition a blink table](../../../conceptual/table-types.md#partition-a-blink-table) for a workflow that uses `remove_blink`.

## Example

The following example applies [`sum_by`](../group-and-aggregate/sumBy.md) to a blink table and to the same table after `remove_blink`.

```python ticking-table order=null
from deephaven import time_table

t_blink = time_table("PT0.5s", blink_table=True).update(
    ["X = k % 5", "Group = k % 2 == 0 ? `A` : `B`"]
)

# Aggregate the blink table
t_blink_sum = t_blink.view(["X", "Group"]).sum_by("Group")

# Aggregate the same table without the blink attribute
t_no_blink = t_blink.remove_blink()
t_no_blink_sum = t_no_blink.view(["X", "Group"]).sum_by("Group")
```

![A blink table's sum_by result accumulating over all rows next to the remove_blink result summing only the current cycle](../../../assets/reference/table-operations/t_blink.gif)

In this example:

- `t_blink_sum` grows continuously because it aggregates every row the blink table has delivered since the `sum_by` was created.
- `t_no_blink_sum` reflects only the sum of rows present in the current update cycle.

With the default one-second [target cycle duration](../../../conceptual/periodic-update-graph-configuration.md), the [blink time table](../../../how-to-guides/time-table.md#create-a-blink-time-table) that [`time_table`](./timeTable.md) creates adds about two rows per cycle, so each group's sum in `t_no_blink_sum` usually covers a single row.

## Related documentation

- [`blink_to_append_only`](./blink-to-append-only.md) — Convert a blink table to append-only to preserve all rows.
- [Table types: Blink tables](../../../conceptual/table-types.md#specialization-3-blink)
- [Table types: Specialized semantics for blink tables](../../../conceptual/table-types.md#specialized-semantics-for-blink-tables) — Detailed explanation of blink table aggregation behavior.
- [Table types: Unsupported operations](../../../conceptual/table-types.md#unsupported-operations)
- [Table types: Partition a blink table](../../../conceptual/table-types.md#partition-a-blink-table)
- [`group_by`](../group-and-aggregate/groupBy.md)
- [`head`](../filter/head.md)
- [`partition_by`](../group-and-aggregate/partitionBy.md)
- [`select_distinct`](../select/select-distinct.md)
- [`sum_by`](../group-and-aggregate/sumBy.md)
- [`tail`](../filter/tail.md)
- [`where_in`](../filter/where-in.md)
- [`where_not_in`](../filter/where-not-in.md)
- [`time_table`](./timeTable.md)
- [Create a blink time table](../../../how-to-guides/time-table.md#create-a-blink-time-table)
- [Table update model](../../../conceptual/table-update-model.md)
- [Special variables](../../query-language/variables/special-variables.md)
- [Periodic Update Graph](../../../conceptual/periodic-update-graph-configuration.md)
- [Pydoc](/core/pydoc/code/deephaven.table.html#deephaven.table.Table.remove_blink)
