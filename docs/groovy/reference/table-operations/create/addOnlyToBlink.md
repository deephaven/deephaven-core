---
title: addOnlyToBlink
---

The `AddOnlyToBlinkTableAdapter.toBlink` method converts an add-only or append-only table to a blink table.

Blink tables keep only the set of rows received during the current update cycle; see [Specialized semantics for blink tables](../../../conceptual/table-types.md#specialized-semantics-for-blink-tables) for details.

> [!NOTE]
> Use this method only on add-only or append-only tables that are not fully in memory, or when you want blink table aggregation semantics. Creating a downstream blink table from a table that is fully in memory is not recommended. It does not achieve the main benefit of blink tables, which is lower memory use. Instead, it increases memory use.

## Syntax

```groovy syntax
import io.deephaven.engine.table.impl.AddOnlyToBlinkTableAdapter

AddOnlyToBlinkTableAdapter.toBlink(table)
```

## Parameters

<ParamTable>
<Param name="table" type="Table">

The add-only or append-only table to convert to a blink table. The table **must** be refreshing.

</Param>
</ParamTable>

## Returns

A blink table based on the input table.

## Examples

The following example creates an append-only time table and converts it to a blink table.

```groovy ticking-table order=null
import io.deephaven.engine.table.impl.AddOnlyToBlinkTableAdapter

source = timeTable("PT1S")

println "Before conversion: ${source.isBlink()}"

result = AddOnlyToBlinkTableAdapter.toBlink(source)

println "After conversion: ${result.isBlink()}"
```

![The above `source` and `result` tables](../../../assets/reference/table-operations/add-only-to-blink-1.gif)

## Related documentation

- [`timeTable`](./timeTable.md)
- [`isBlink`](../metadata/isBlink.md)
- [`removeBlink`](./remove-blink.md)
- [`blinkToAppendOnly`](./blink-to-append-only.md)
- [Table types: Specialized semantics for blink tables](../../../conceptual/table-types.md#specialized-semantics-for-blink-tables)
- [Javadoc](/core/javadoc/io/deephaven/engine/table/impl/AddOnlyToBlinkTableAdapter.html#toBlink(io.deephaven.engine.table.Table))
