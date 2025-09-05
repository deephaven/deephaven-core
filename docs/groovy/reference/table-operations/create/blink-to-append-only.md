---
title: blinkToAppendOnly
---

The `BlinkTableTools.blinkToAppendOnly` method creates an append-only table from the supplied blink table.

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

The blink table to convert to an append-only table.

</Param>
<Param name="sizeLimit" type="long">

The maximum number of rows in the resulting append-only table.

</Param>
<Param name="memoKey" type="Object">

Saves a weak reference to the result of the given operation under the given memoization key. Set to `null` to disable memoization.

</Param>
</ParamTable>

## Returns

An append-only table.

## Example

The following example creates a blink table with a [`TimeTable.Builder`](/core/javadoc/io/deephaven/engine/table/impl/TimeTable.Builder.html) and then converts the blink table to an append-only table with `blinkToAppendOnly`.

```groovy order=null
import io.deephaven.engine.table.impl.TimeTable.Builder
import io.deephaven.engine.table.impl.BlinkTableTools

builder = new Builder().period("PT2S").blinkTable(true)

tt1 = builder.build()

appendOnlyResult = BlinkTableTools.blinkToAppendOnly(tt1)
```

![The above `tt1` and `appendOnlyResult` tables ticking side-by-side](../../../assets/reference/create/blink-to-append-only-groovy.gif)

## Related Documentation

- [`timeTable`](./timeTable.md)
