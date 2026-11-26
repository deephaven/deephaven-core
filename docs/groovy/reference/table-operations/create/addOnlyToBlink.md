---
title: AddOnlyToBlinkTableAdapter.toBlink
---

The `AddOnlyToBlinkTableAdapter.toBlink` method converts an [add-only](../../../conceptual/table-types.md#specialization-2-add-only) or [append-only](../../../conceptual/table-types.md#specialization-1-append-only) table to a [blink table](../../../conceptual/table-types.md#specialization-3-blink). At creation, the blink table contains all rows currently in the source table; in each later [update cycle](../../../conceptual/table-update-model.md), it contains only the rows added in that cycle.

> [!NOTE]
> Use this method only on add-only or append-only tables that are not fully in memory, or when [blink-table aggregation semantics](../../../conceptual/table-types.md#specialized-semantics-for-blink-tables) are needed. If the source table holds all its rows in memory, the conversion increases memory use instead of reducing it: the source table still keeps every row, and the blink table adds its own row set and listener.

## Syntax

```groovy syntax
import io.deephaven.engine.table.impl.AddOnlyToBlinkTableAdapter

AddOnlyToBlinkTableAdapter.toBlink(table)
```

## Parameters

<ParamTable>
<Param name="table" type="Table">

The add-only or append-only table to convert to a blink table. The table **must** be refreshing; passing a static table throws an `IllegalArgumentException`. The table must also only ever add rows. `toBlink` does not check this at call time. If the source later removes, modifies, or shifts rows, that update fails with an error.

</Param>
</ParamTable>

## Returns

A blink table. If `table` is already a blink table, `toBlink` returns it unchanged.

## Example

The following example creates an append-only table with [`timeTable`](./timeTable.md), converts it to a blink table with `AddOnlyToBlinkTableAdapter.toBlink`, and prints the value of [`isBlink`](../metadata/isBlink.md) before and after the conversion.

```groovy ticking-table order=:log
import io.deephaven.engine.table.impl.AddOnlyToBlinkTableAdapter

source = timeTable("PT1S")

println "Before conversion: ${source.isBlink()}"

result = AddOnlyToBlinkTableAdapter.toBlink(source)

println "After conversion: ${result.isBlink()}"
```

The animation shows `source` keeping every row it has added, while `result` holds only the rows added in the current update cycle.

![The above `source` and `result` tables ticking side by side in the Deephaven console](../../../assets/reference/table-operations/add-only-to-blink-1.gif)

A time table computes each timestamp from its row key and stores no per-row data, so converting it saves no memory; the example only illustrates the syntax. To create a blink time table directly, call [`timeTableBuilder`](/core/javadoc/io/deephaven/engine/util/TableTools.html#timeTableBuilder()) and pass `true` to [`blinkTable`](/core/javadoc/io/deephaven/engine/table/impl/TimeTable.Builder.html#blinkTable(boolean)), as shown in [Create a blink time table](../../../how-to-guides/time-table.md#create-a-blink-time-table).

## Related documentation

- [Convert add- and append-only tables to blink tables](../../../how-to-guides/add-only-to-blink.md)
- [`timeTable`](./timeTable.md)
- [Create a blink time table](../../../how-to-guides/time-table.md#create-a-blink-time-table)
- [`isBlink`](../metadata/isBlink.md)
- [`removeBlink`](./remove-blink.md)
- [`blinkToAppendOnly`](./blink-to-append-only.md)
- [Table types: Specialized semantics for blink tables](../../../conceptual/table-types.md#specialized-semantics-for-blink-tables)
- [Incremental update model](../../../conceptual/table-update-model.md)
- [`timeTableBuilder`](/core/javadoc/io/deephaven/engine/util/TableTools.html#timeTableBuilder())
- [`TimeTable.Builder.blinkTable`](/core/javadoc/io/deephaven/engine/table/impl/TimeTable.Builder.html#blinkTable(boolean))
- [Javadoc](/core/javadoc/io/deephaven/engine/table/impl/AddOnlyToBlinkTableAdapter.html#toBlink(io.deephaven.engine.table.Table))
