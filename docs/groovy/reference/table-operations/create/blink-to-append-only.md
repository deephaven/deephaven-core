---
title: blinkToAppendOnly
---

The `BlinkTableTools.blinkToAppendOnly` method creates an [append-only table](../../../conceptual/table-types.md#specialization-1-append-only) from the supplied [blink table](../../../conceptual/table-types.md#specialization-3-blink).

## Syntax

```groovy syntax
import io.deephaven.engine.table.impl.BlinkTableTools

BlinkTableTools.blinkToAppendOnly(blinkTable)
BlinkTableTools.blinkToAppendOnly(blinkTable, sizeLimit)
BlinkTableTools.blinkToAppendOnly(blinkTable, memoKey)
BlinkTableTools.blinkToAppendOnly(blinkTable, sizeLimit, memoKey)
```

## Parameters

<ParamTable>
<Param name="blinkTable" type="Table">

The blink table to convert to an append-only table. The table must be a blink table; otherwise, the method throws an `IllegalArgumentException`.

</Param>
<Param name="sizeLimit" type="long">

The maximum number of rows in the resulting append-only table. A negative value throws an `IllegalArgumentException`. A value of `0` returns a static, empty table with the blink table's columns.

</Param>
<Param name="memoKey" type="Object">

The memoization key. While an earlier result is still in use, a call with the same blink table, the same size limit, and an equal key returns that result table instead of creating a new one, unless the `QueryTable.memoizeResults` configuration property is `false`. Set to `null` to disable memoization. Overloads without this parameter memoize under [`BlinkTableTools.DEFAULT_MEMO_KEY`](/core/javadoc/io/deephaven/engine/table/impl/BlinkTableTools.html#DEFAULT_MEMO_KEY).

</Param>
</ParamTable>

## Returns

An in-memory, append-only table that contains the rows the blink table holds at the time of the call and every row the blink table adds after that. Without `sizeLimit`, the table grows without bound as new rows arrive. With `sizeLimit`, the table keeps only the first `sizeLimit` rows it receives, counting the rows present at the time of the call, and ignores every row after that.

## Examples

The following example creates a [blink time table](../../../how-to-guides/time-table.md#create-a-blink-time-table) with [`timeTableBuilder`](/core/javadoc/io/deephaven/engine/util/TableTools.html#timeTableBuilder()) and then converts the blink table to an append-only table with `blinkToAppendOnly`.

```groovy ticking-table order=null
import io.deephaven.engine.table.impl.BlinkTableTools

tt1 = timeTableBuilder().period("PT2S").blinkTable(true).build()

appendOnlyResult = BlinkTableTools.blinkToAppendOnly(tt1)
```

![The above `tt1` and `appendOnlyResult` tables ticking side by side](../../../assets/reference/create/blink-to-append-only-groovy.gif)

The following example passes a `sizeLimit` of 10. The resulting table stops growing once it holds 10 rows.

```groovy ticking-table order=null
import io.deephaven.engine.table.impl.BlinkTableTools

tt1 = timeTableBuilder().period("PT2S").blinkTable(true).build()

appendOnlyLimited = BlinkTableTools.blinkToAppendOnly(tt1, 10)
```

## Related documentation

- [`timeTable`](./timeTable.md)
- [`timeTableBuilder`](/core/javadoc/io/deephaven/engine/util/TableTools.html#timeTableBuilder())
- [Create a time table](../../../how-to-guides/time-table.md)
- [`isBlink`](../metadata/isBlink.md)
- [`removeBlink`](./remove-blink.md)
- [`AddOnlyToBlinkTableAdapter.toBlink`](./addOnlyToBlink.md)
- [Table types: Specialized semantics for blink tables](../../../conceptual/table-types.md#specialized-semantics-for-blink-tables)
- [Javadoc](/core/javadoc/io/deephaven/engine/table/impl/BlinkTableTools.html#blinkToAppendOnly(io.deephaven.engine.table.Table))
