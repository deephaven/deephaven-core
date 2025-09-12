---
title: slice
---

The `slice` method returns a table that is a subset of another table based on row positions.

> [!CAUTION]
> Attempting to use `slice` on a [blink table](../../../conceptual/table-types.md#specialization-3-blink) will raise an error.

## Syntax

```
table.slice(firstPositionInclusive, lastPositionExclusive)
```

## Parameters

<ParamTable>
<Param name="firstPositionInclusive" type="long">

The row index at which the slice starts. If positive, the row index is counted down from the top of the table. If negative, the row index is counted up from the bottom of the table. This index is inclusive.

</Param>
<Param name="lastPositionExclusive" type="long">

The row index at which the slice ends. If positive, the row index is counted down from the top of the table. If negative, the row index is counted up from the bottom of the table. This index is not inclusive.

</Param>
</ParamTable>

## Returns

A new table with a number of rows corresponding to the difference between the start and end row indices. If the start index is negative, the end index must also be less than or equal to 0, and greater than or equal to the start index. If these conditions are not met, an error will occur.

## Example

The following example filters the table to the middle 50 rows.

```groovy order=source,result
source = emptyTable(100).update("X = i")
result = source.slice(25, 75)
```

The following example uses negative start and end indices to filter the table to those from the fifth-to-last to the second-to-last.

```groovy order=source,result
source = emptyTable(100).update("X = i")
result = source.slice(-5, -1)
```

The following example uses a positive start index and negative end index to filter out rows from the start and end of the table.

```groovy order=source,result
source = emptyTable(100).update("X = i")
result = source.slice(10, -10)
```

The following example uses a negative start index and positive end index to return all rows starting from the third-last row to the fifth row of the table.

```groovy order=source,result
source = emptyTable(5).update("X = i")
result = source.slice(-3, 5)
```

The following example shows how the use of row index 0 as the start or end index filters a table to its first or last N rows.

```groovy order=source,sliceHead,head,sliceTail,tail
source=emptyTable(100).update("X = i")
sliceHead = source.slice(0, 20)
head = source.head(20)
sliceTail = source.slice(-20, 0)
tail = source.tail(20)
```

## Related documentation

- [Create an empty table](../../../how-to-guides/new-and-empty-table.md#emptytable)
- [How to use filters](../../../how-to-guides/filters.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/api/TableOperations.html#slice(long,long))
