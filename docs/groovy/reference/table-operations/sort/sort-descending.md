---
title: sortDescending
---

`sortDescending` sorts rows in a table in a largest to smallest order based on the column(s) listed in the `columnsToSortBy` argument.

## Syntax

```
table.sortDescending(columnsToSortBy...)
```

## Parameters

<ParamTable>
<Param name="columnsToSortBy" type="String...">

The column(s) used for sorting.

</Param>
</ParamTable>

## Returns

A new table where rows in a table are sorted in a largest to smallest order based on the column(s) listed in the `columnsToSortBy` argument.

## Examples

```groovy order=source,result
source = newTable(
    stringCol("Letter", "A", "C", "F", "B", "E", "D"),
    intCol("Number", 6, 2, 1, 3, 4, 5),
)

result = source.sortDescending("Letter")
```

```groovy order=source,result
source = newTable(
    stringCol("Letter", "A", "B", "A", "B", "B", "A"),
    intCol("Number", 6, 2, 1, 3, 4, 5),
    stringCol("Color", "red", "blue", "orange", "purple", "yellow", "pink")
)

result = source.sortDescending("Letter", "Number")
```

## Related documentation

- [Create a new table](../../../how-to-guides/new-and-empty-table.md#newtable)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/api/TableOperations.html#sortDescending(java.lang.String...))
