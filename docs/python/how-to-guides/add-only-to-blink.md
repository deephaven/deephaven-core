---
title: Convert add- and append-only tables to blink tables
---

This guide shows you how to convert a refreshing [add-only](../conceptual/table-types.md#specialization-2-add-only) or [append-only](../conceptual/table-types.md#specialization-1-append-only) table to a [blink table](../conceptual/table-types.md#specialization-3-blink), which keeps only the rows added during the current [update cycle](../conceptual/table-update-model.md). It's a simple process that takes a single function call: [`add_only_to_blink`](../reference/table-operations/create/add-only-to-blink.md).

The source table must meet two requirements:

- It must be refreshing, not [static](../conceptual/table-types.md#static-tables). [`add_only_to_blink`](../reference/table-operations/create/add-only-to-blink.md) raises a `DHError` when you pass a static table.
- It must only ever add rows. [`add_only_to_blink`](../reference/table-operations/create/add-only-to-blink.md) doesn't check this when you call it. If the source later removes, modifies, or shifts rows, that update fails with an error.

If the source is already a blink table, [`add_only_to_blink`](../reference/table-operations/create/add-only-to-blink.md) returns it unchanged.

## When to use this function

Convert an [add-only](../conceptual/table-types.md#specialization-2-add-only) or [append-only](../conceptual/table-types.md#specialization-1-append-only) table to a [blink table](../conceptual/table-types.md#specialization-3-blink) when:

- You have an [add-only](../conceptual/table-types.md#specialization-2-add-only) or [append-only](../conceptual/table-types.md#specialization-1-append-only) source table that is not fully in memory. Deephaven reads its column data from storage on demand instead of keeping every row in memory. Operations that keep a result for every row, such as [`update`](../reference/table-operations/select/update.md), hold only the current cycle's rows when they run on the converted [blink table](../conceptual/table-types.md#specialization-3-blink), so they use less memory than on the source. Aggregations such as [`count_by`](../reference/table-operations/group-and-aggregate/countBy.md) keep the same per-group state either way. Lower memory use is the main benefit of [blink tables](../conceptual/table-types.md#specialization-3-blink).
- You want [blink-table aggregation semantics](../conceptual/table-types.md#specialized-semantics-for-blink-tables). An [aggregation](../conceptual/aggregations.md) such as [`sum_by`](../reference/table-operations/group-and-aggregate/sumBy.md) or [`count_by`](../reference/table-operations/group-and-aggregate/countBy.md) on a [blink table](../conceptual/table-types.md#specialization-3-blink) covers every row the table has delivered since the [aggregation](../conceptual/aggregations.md) started. The table itself holds only the rows from the current [update cycle](../conceptual/table-update-model.md).

Converting a table that is already fully in memory increases memory use rather than reducing it.

Some operations, such as [`group_by`](../reference/table-operations/group-and-aggregate/groupBy.md) and [`partition_by`](../reference/table-operations/group-and-aggregate/partitionBy.md), [raise an error on a blink table](../conceptual/table-types.md#unsupported-operations).

## Example

In this example, we create a [time table](../how-to-guides/time-table.md) that refreshes every second, then convert it to a [blink table](../conceptual/table-types.md#specialization-3-blink) using [`add_only_to_blink`](../reference/table-operations/create/add-only-to-blink.md). The [time table](../how-to-guides/time-table.md) computes each timestamp from the row key and stores no per-row data, so converting it saves no memory. It's still a refreshing [append-only](../conceptual/table-types.md#specialization-1-append-only) source, so the example shows the syntax. The `counts` table shows the [blink-table aggregation semantics](../conceptual/table-types.md#specialized-semantics-for-blink-tables) described under [When to use this function](#when-to-use-this-function).

```python ticking-table order=null
from deephaven.stream import add_only_to_blink
from deephaven import time_table

# create source table
source = time_table("PT1S")

# convert `source` table to a blink table
result = add_only_to_blink(source)

# count every row `result` has delivered
counts = result.count_by("Count")
```

`source` keeps every row the [time table](../how-to-guides/time-table.md) has added, while `result` contains only the rows added during the most recent [update cycle](../conceptual/table-update-model.md). When you first create `result`, it holds all the rows `source` already has, and it drops them on the next [update cycle](../conceptual/table-update-model.md).

![Three tables tick side by side. `source` grows by one row each second. `result` holds only the newest `source` row, and `Count` in `counts` keeps growing](../assets/how-to/add-only-to-blink.gif)

The `Count` value in `counts` keeps growing even though `result` holds only the latest rows. That's [blink-table aggregation semantics](../conceptual/table-types.md#specialized-semantics-for-blink-tables): [`count_by`](../reference/table-operations/group-and-aggregate/countBy.md) counts every row `result` has held since you created `counts`. The count starts with the rows `result` held when you created `counts`.

If all you need is a blink time table, you don't need [`add_only_to_blink`](../reference/table-operations/create/add-only-to-blink.md). Create one directly with [`time_table`](../reference/table-operations/create/timeTable.md) and `blink_table=True`, as shown in [Create a blink time table](../how-to-guides/time-table.md#create-a-blink-time-table).

## Related documentation

- [Table types](../conceptual/table-types.md)
- [Incremental update model](../conceptual/table-update-model.md)
- [Aggregations in Deephaven](../conceptual/aggregations.md)
- [Create a time table](../how-to-guides/time-table.md)
- [`add_only_to_blink`](../reference/table-operations/create/add-only-to-blink.md)
- [`blink_to_append_only`](../reference/table-operations/create/blink-to-append-only.md)
- [`count_by`](../reference/table-operations/group-and-aggregate/countBy.md)
- [`group_by`](../reference/table-operations/group-and-aggregate/groupBy.md)
- [`partition_by`](../reference/table-operations/group-and-aggregate/partitionBy.md)
- [`sum_by`](../reference/table-operations/group-and-aggregate/sumBy.md)
- [`is_blink`](../reference/table-operations/metadata/is_blink.md)
- [`time_table`](../reference/table-operations/create/timeTable.md)
- [Pydoc](/core/pydoc/code/deephaven.stream.html#deephaven.stream.add_only_to_blink)
