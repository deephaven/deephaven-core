---
title: removeBlink
---

The `removeBlink` method removes the blink table attribute from a table, disabling [specialized blink table aggregation semantics](../../../conceptual/table-types.md#specialized-semantics-for-blink-tables). It returns the source table itself if it is already not a blink table.

> [!NOTE]
> `removeBlink` only removes the blink _attribute_ — **rows will still disappear each update cycle**. If you want rows to persist, use [`blinkToAppendOnly`](./blink-to-append-only.md) instead.

**What it does:**

- Disables [specialized aggregation semantics](../../../conceptual/table-types.md#specialized-semantics-for-blink-tables) for blink tables. By default, aggregations like [`sumBy`](../group-and-aggregate/sumBy.md) on blink tables accumulate results over the _entire history_ of observed rows. After calling `removeBlink`, aggregations only operate on rows present in the current update cycle.

**What it does NOT do:**

- It does **not** make rows persist. The resulting table still exhibits the "blink" update pattern — all rows from the previous cycle are removed, and only new rows appear each cycle.
- It does **not** convert the table to an append-only or standard streaming table.

## Syntax

```groovy syntax
table.removeBlink()
```

## Parameters

This method takes no arguments.

## Returns

A table without the blink attribute. The table still removes all rows each update cycle.

## Example

The following example demonstrates the difference in aggregation behavior. With the blink attribute, `sumBy` accumulates over all rows ever seen. After `removeBlink`, `sumBy` only sums rows in the current cycle.

```groovy ticking-table order=tBlinkSum,tNoBlinkSum
import io.deephaven.engine.table.impl.TimeTable.Builder

builder = new Builder().period("PT0.5s").blinkTable(true)

tBlink = builder.build().update("X = ii % 5", "Group = ii % 2 == 0 ? `A` : `B`")

// With blink attribute: sum accumulates over ALL rows ever seen
tBlinkSum = tBlink.view("X", "Group").sumBy("Group")

// After removeBlink: sum only includes rows in the CURRENT cycle
tNoBlink = tBlink.removeBlink()
tNoBlinkSum = tNoBlink.view("X", "Group").sumBy("Group")
```

![t_blink](../../../assets/reference/create/t_blink.gif)

In this example:

- `tBlinkSum` grows continuously as it aggregates over all historical data.
- `tNoBlinkSum` only reflects the sum of rows present in the current update cycle (which is just one row per group per cycle in this case).

## Related documentation

- [blinkToAppendOnly](./blink-to-append-only.md) — Convert a blink table to append-only to preserve all rows.
- [Table types: Specialized semantics for blink tables](../../../conceptual/table-types.md#specialized-semantics-for-blink-tables) — Detailed explanation of blink table aggregation behavior.
- [Javadoc](<https://deephaven.io/core/javadoc/io/deephaven/engine/table/Table.html#removeBlink()>)
