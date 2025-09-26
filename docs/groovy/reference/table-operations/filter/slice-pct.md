---
title: slicePct
---

The `slicePct` method returns a table that that is a subset of another table corresponding to the difference between the start and end row percentages. For example, for a table of size 10, `slicePct(0.1, 0.7)` will return a subset from the second row to the seventh row. Similarly, `slicePct(0, 1)` would return the entire table (because row positions run from 0 to size - 1). If the percentage arguments are outisde of the range [0, 1], an error will occur.

> [!CAUTION]
> Attempting to use `slicePct` on a [blink table](../../../conceptual/table-types.md#specialization-3-blink) will raise an error.

## Syntax

```
table.slicePct(startPercentInclusive, endPercentExclusive)
```

## Parameters

<ParamTable>
<Param name="startPercentInclusive" type="double">

The starting percentage point (inclusive) for rows to include in the result. The percentage arguments must be in the range [0.0, 1.0].

</Param>
<Param name="endPercentExclusive" type="double">

The ending percentage point (exclusive) for rows to include in the result. The percentage arguments must be in the range [0, 1].

</Param>
</ParamTable>

## Returns

A new table that is a subset of the source table.

## Example

The following example filters the table to a subset between the tenth and 70th rows.

```groovy order=source,result
source = emptyTable(100).update("X = i")
result = source.slicePct(0.1, 0.7)
```

## Related documentation

- [Create an empty table](../../../how-to-guides/new-and-empty-table.md#emptytable)
- [How to use filters](../../../how-to-guides/filters.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/engine/table/Table.html#slicePct(double,double))
