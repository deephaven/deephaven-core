---
title: removeBlink
---

The `removeBlink` method removes the blink attribute from a [blink table](../../../conceptual/table-types.md#specialization-3-blink). The blink attribute is a marker on the table that tells Deephaven to apply blink table semantics, including [specialized aggregation semantics](../../../conceptual/table-types.md#specialized-semantics-for-blink-tables). Removing it turns those semantics off, so later operations on the result treat it as an ordinary refreshing table. [Effects](#effects) lists what changes.

> [!NOTE]
> `removeBlink` only removes the blink _attribute_ — **rows still disappear each update cycle**. Each cycle removes all rows from the previous cycle, and only new rows appear, so the result is not an append-only table. To keep rows across update cycles, use [`blinkToAppendOnly`](./blink-to-append-only.md) instead.

## Syntax

```groovy syntax
table.removeBlink()
```

## Parameters

This method takes no arguments.

## Returns

A table without the blink attribute, or the source table itself if it is not a blink table.

## Effects

Operations on the result behave as follows:

- Aggregations such as [`sumBy`](../group-and-aggregate/sumBy.md) operate only on the rows in the current [update cycle](../../../conceptual/table-update-model.md) instead of on every row the blink table has delivered since the aggregation was created. The same applies to other operations that treat blink tables specially, such as [`selectDistinct`](../select/select-distinct.md), [`head`](../filter/head.md), and [`tail`](../filter/tail.md). When a blink table is the filter table for [`whereIn`](../filter/where-in.md) or [`whereNotIn`](../filter/where-not-in.md), removing the attribute makes the filter use only the current cycle's keys.
- Operations that throw an error on a blink table, such as [`groupBy`](../group-and-aggregate/groupBy.md) and [`partitionBy`](../group-and-aggregate/partitionBy.md), run on the result and operate only on the rows of the current update cycle.
- Formulas that use the [special variables](../../query-language/variables/special-variables.md) `i`, `ii`, or `k`, or column array expressions other than a simple constant offset such as `X_[i-1]`, throw an error when applied to the result, because the result is no longer marked as holding only the current cycle's rows. Apply such formulas before calling `removeBlink`, as the [example](#example) does.

See [Unsupported operations](../../../conceptual/table-types.md#unsupported-operations) for the full list of operations that a blink table does not support, and [Partition a blink table](../../../conceptual/table-types.md#partition-a-blink-table) for a workflow that uses `removeBlink`.

## Example

The following example applies [`sumBy`](../group-and-aggregate/sumBy.md) to a blink table and to the same table after `removeBlink`.

```groovy ticking-table order=null
tBlink = timeTableBuilder().period("PT0.5s").blinkTable(true).build()
    .update("X = k % 5", "Group = k % 2 == 0 ? `A` : `B`")

// Aggregate the blink table
tBlinkSum = tBlink.view("X", "Group").sumBy("Group")

// Aggregate the same table without the blink attribute
tNoBlink = tBlink.removeBlink()
tNoBlinkSum = tNoBlink.view("X", "Group").sumBy("Group")
```

![A blink table's sumBy result accumulating over all rows next to the removeBlink result summing only the current cycle](../../../assets/reference/create/t_blink.gif)

In this example:

- `tBlinkSum` grows continuously because it aggregates every row the blink table has delivered since the `sumBy` was created.
- `tNoBlinkSum` reflects only the sum of rows present in the current update cycle.

With the default one-second [target cycle duration](../../../conceptual/periodic-update-graph-configuration.md), the [blink time table](../../../how-to-guides/time-table.md#create-a-blink-time-table) that [`timeTableBuilder`](/core/javadoc/io/deephaven/engine/util/TableTools.html#timeTableBuilder()) creates adds about two rows per cycle, so each group's sum in `tNoBlinkSum` usually covers a single row.

## Related documentation

- [`blinkToAppendOnly`](./blink-to-append-only.md) — Convert a blink table to append-only to preserve all rows.
- [Table types: Blink tables](../../../conceptual/table-types.md#specialization-3-blink)
- [Table types: Specialized semantics for blink tables](../../../conceptual/table-types.md#specialized-semantics-for-blink-tables) — Detailed explanation of blink table aggregation behavior.
- [Table types: Unsupported operations](../../../conceptual/table-types.md#unsupported-operations)
- [Table types: Partition a blink table](../../../conceptual/table-types.md#partition-a-blink-table)
- [`groupBy`](../group-and-aggregate/groupBy.md)
- [`head`](../filter/head.md)
- [`partitionBy`](../group-and-aggregate/partitionBy.md)
- [`selectDistinct`](../select/select-distinct.md)
- [`sumBy`](../group-and-aggregate/sumBy.md)
- [`tail`](../filter/tail.md)
- [`whereIn`](../filter/where-in.md)
- [`whereNotIn`](../filter/where-not-in.md)
- [`timeTableBuilder`](/core/javadoc/io/deephaven/engine/util/TableTools.html#timeTableBuilder())
- [Create a blink time table](../../../how-to-guides/time-table.md#create-a-blink-time-table)
- [Table update model](../../../conceptual/table-update-model.md)
- [Special variables](../../query-language/variables/special-variables.md)
- [Periodic Update Graph](../../../conceptual/periodic-update-graph-configuration.md)
- [Javadoc](/core/javadoc/io/deephaven/engine/table/Table.html#removeBlink())
