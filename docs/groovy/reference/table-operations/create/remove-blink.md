---
title: removeBlink
---

The `removeBlink` method returns a new non-blink copy of the source table or the source table itself if it is already not a blink table. This is useful when you want to disable the default aggregation <!--TODO: link conceptual/table-types#specialized semantics when merged--> behavior of a blink table.

By default, many aggregations on blink tables will be computed over _every row_ that the table has seen since the aggregation was called. Although data in a blink table disappears every update cycle, some aggregations can still be computed as though the whole data history were available.

## Syntax

```groovy syntax
table.removeBlink()
```

## Parameters

This method takes no arguments.

## Returns

A new non-blink Table.

## Example

The following example creates a simple blink table with the time table builder and then calls `removeBlink` to make it a non-blink table. We then demonstrate the difference in aggregation behavior between a blink table and a non-blink table by summing the `X` column in each table.

```groovy order=tNoBlink,tBlink
import io.deephaven.engine.table.impl.TimeTable.Builder
import io.deephaven.engine.table.impl.BlinkTableTools

builder = new Builder().period("PT2S").blinkTable(true)

tBlink = builder.build()
tNoBlink = tBlink.removeBlink()
```

## Related documentation

- [assertBlink](assert-blink.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/engine/table/Table.html#removeBlink())
