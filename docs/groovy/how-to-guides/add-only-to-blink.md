---
title: Convert add- and append-only tables to blink tables
---

This guide shows you how to convert a refreshing [add-only](../conceptual/table-types.md#specialization-2-add-only) or [append-only](../conceptual/table-types.md#specialization-1-append-only) table to a [blink table](../conceptual/table-types.md#specialization-3-blink), which keeps only the rows added during the current [update cycle](../conceptual/table-update-model.md). It's a simple process that takes a single method call: [`AddOnlyToBlinkTableAdapter.toBlink`](../reference/table-operations/create/addOnlyToBlink.md).

The source table must meet two requirements:

- It must be refreshing, not [static](../conceptual/table-types.md#static-tables). [`AddOnlyToBlinkTableAdapter.toBlink`](../reference/table-operations/create/addOnlyToBlink.md) throws an `IllegalArgumentException` when you pass a static table.
- It must only ever add rows. [`AddOnlyToBlinkTableAdapter.toBlink`](../reference/table-operations/create/addOnlyToBlink.md) doesn't check this when you call it. If the source later removes, modifies, or shifts rows, that update fails with an error.

If the source is already a blink table, [`AddOnlyToBlinkTableAdapter.toBlink`](../reference/table-operations/create/addOnlyToBlink.md) returns it unchanged.

## When to use this method

Convert an [add-only](../conceptual/table-types.md#specialization-2-add-only) or [append-only](../conceptual/table-types.md#specialization-1-append-only) table to a [blink table](../conceptual/table-types.md#specialization-3-blink) when:

- You have an [add-only](../conceptual/table-types.md#specialization-2-add-only) or [append-only](../conceptual/table-types.md#specialization-1-append-only) source table that is not fully in memory. Deephaven reads its column data from storage on demand instead of keeping every row in memory. Operations on the converted [blink table](../conceptual/table-types.md#specialization-3-blink) work on only the current cycle's rows, so they use less memory than the same operations on the source. Lower memory use is the main benefit of [blink tables](../conceptual/table-types.md#specialization-3-blink).
- You want [blink-table aggregation semantics](../conceptual/table-types.md#specialized-semantics-for-blink-tables). An [aggregation](../conceptual/aggregations.md) such as [`sumBy`](../reference/table-operations/group-and-aggregate/sumBy.md) or [`countBy`](../reference/table-operations/group-and-aggregate/countBy.md) on a [blink table](../conceptual/table-types.md#specialization-3-blink) covers every row the table has delivered since the [aggregation](../conceptual/aggregations.md) started. The table itself holds only the rows from the current [update cycle](../conceptual/table-update-model.md).

Converting a table that is already fully in memory increases memory use rather than reducing it.

Some operations, such as [`groupBy`](../reference/table-operations/group-and-aggregate/groupBy.md) and [`partitionBy`](../reference/table-operations/group-and-aggregate/partitionBy.md), [raise an error on a blink table](../conceptual/table-types.md#unsupported-operations).

## Example

In this example, we create a [time table](../how-to-guides/time-table.md) that refreshes every second, then convert it to a [blink table](../conceptual/table-types.md#specialization-3-blink) using [`AddOnlyToBlinkTableAdapter.toBlink`](../reference/table-operations/create/addOnlyToBlink.md). The [time table](../how-to-guides/time-table.md) computes each timestamp from the row key and stores no per-row data, so converting it saves no memory. It's still a refreshing [append-only](../conceptual/table-types.md#specialization-1-append-only) source, so the example shows the syntax. The `counts` table shows the [blink-table aggregation semantics](../conceptual/table-types.md#specialized-semantics-for-blink-tables) described under [When to use this method](#when-to-use-this-method).

```groovy ticking-table order=null
import io.deephaven.engine.table.impl.AddOnlyToBlinkTableAdapter

// create source table
source = timeTable("PT1S")

// convert `source` table to a blink table
result = AddOnlyToBlinkTableAdapter.toBlink(source)

// count every row `result` has delivered
counts = result.countBy("Count")
```

`source` keeps every row the [time table](../how-to-guides/time-table.md) has added, while `result` contains only the rows added during the most recent [update cycle](../conceptual/table-update-model.md). When you first create `result`, it holds all the rows `source` already has, and it drops them on the next [update cycle](../conceptual/table-update-model.md).

![Three tables tick side by side. `source` grows by one row each second. `result` holds only the newest `source` row, and `Count` in `counts` keeps growing](../assets/how-to/add-only-to-blink.gif)

The `Count` value in `counts` keeps growing even though `result` holds only the latest rows. That's [blink-table aggregation semantics](../conceptual/table-types.md#specialized-semantics-for-blink-tables): [`countBy`](../reference/table-operations/group-and-aggregate/countBy.md) counts every row `result` has held since you created `counts`. The count starts with the rows `result` held when you created `counts`.

If all you need is a blink time table, you don't need [`AddOnlyToBlinkTableAdapter.toBlink`](../reference/table-operations/create/addOnlyToBlink.md). Create one directly with `timeTableBuilder`. Call `blinkTable(true)` on the [`TimeTable.Builder`](/core/javadoc/io/deephaven/engine/table/impl/TimeTable.Builder.html) it returns, as shown in [Create a blink time table](../how-to-guides/time-table.md#create-a-blink-time-table).

## Related documentation

- [Table types](../conceptual/table-types.md)
- [Incremental update model](../conceptual/table-update-model.md)
- [Aggregations in Deephaven](../conceptual/aggregations.md)
- [Create a time table](../how-to-guides/time-table.md)
- [`addOnlyToBlink`](../reference/table-operations/create/addOnlyToBlink.md)
- [`blinkToAppendOnly`](../reference/table-operations/create/blink-to-append-only.md)
- [`countBy`](../reference/table-operations/group-and-aggregate/countBy.md)
- [`groupBy`](../reference/table-operations/group-and-aggregate/groupBy.md)
- [`partitionBy`](../reference/table-operations/group-and-aggregate/partitionBy.md)
- [`sumBy`](../reference/table-operations/group-and-aggregate/sumBy.md)
- [`isBlink`](../reference/table-operations/metadata/isBlink.md)
- [`timeTable`](../reference/table-operations/create/timeTable.md)
- [Javadoc](/core/javadoc/io/deephaven/engine/table/impl/AddOnlyToBlinkTableAdapter.html)
