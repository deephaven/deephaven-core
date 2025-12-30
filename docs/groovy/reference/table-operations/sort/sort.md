---
title: sort
slug: ./sort
---

`sort` sorts rows in a table in a smallest to largest order based on the column(s) listed in the `columnsToSortBy` argument.

The `sort` method can also accept `SortColumn` pairs, which allow some columns to be sorted in ascending order while other columns are sorted in descending order.

## Syntax

```
table.sort(columnsToSortBy...)
```

## Parameters

<ParamTable>
<Param name="columnsToSortBy" type="String...">

The column(s) to sort in ascending order.

</Param>
<Param name="columnsToSortBy" type="Collection<SortColumn>">

A columns and directions to sort by.

</Param>

</ParamTable>

## Returns

A new table where (1) rows are sorted in a smallest to largest order based on the column(s) listed in the `columnsToSortBy` argument or (2) where rows are sorted in the order defined by the `SortColumns` listed in the `sort` argument.

## Examples

In the following example, `sort` will return a new table with the `Letter` column sorted in ascending order.

```groovy order=source,result
source = newTable(
    stringCol("Letter", "A", "B", "A", "B", "B", "A"),
    intCol("Number", 6, 6, 1, 3, 4, 4),
    stringCol("Color", "red", "blue", "orange", "purple", "yellow", "pink")
)

result = source.sort("Letter")
```

In the following example, the `Number` column is first sorted in ascending order, _then_ the `Letter` column is sorted in ascending order.

```groovy order=source,result
source = newTable(
    stringCol("Letter", "A", "B", "A", "B", "B", "A"),
    intCol("Number", 6, 6, 1, 3, 4, 4),
    stringCol("Color", "red", "blue", "orange", "purple", "yellow", "pink")
)

result = source.sort("Number", "Letter")
```

In the following example, `sort` accepts objects returned from `sort_columns`. The `Number` column is first sorted in ascending order, _then_ the `Letter` column is sorted in descending order.

```groovy order=source,result
import io.deephaven.api.SortColumn
import io.deephaven.api.ColumnName

source = newTable(
    stringCol("Letter", "A", "B", "A", "B", "B", "A"),
    intCol("Number", 6, 6, 1, 3, 4, 4),
    stringCol("Color", "red", "blue", "orange", "purple", "yellow", "pink")
)

sort_columns = [
    SortColumn.asc(ColumnName.of("Number")),
    SortColumn.desc(ColumnName.of("Letter"))
]

result = source.sort(sort_columns)
```

The following example returns the same table as the prior example. The `Number` column is first sorted in ascending order, _then_ the `Letter` column is sorted in descending order.

```groovy order=source,result
source = newTable(
    stringCol("Letter", "A", "B", "A", "B", "B", "A"),
    intCol("Number", 6, 6, 1, 3, 4, 4),
    stringCol("Color", "red", "blue", "orange", "purple", "yellow", "pink")
)

result = source.sortDescending("Letter").sort("Number")
```

## Related documentation

- [Create a new table](../../../how-to-guides/new-and-empty-table.md#newtable)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/api/TableOperations.html#sort(java.lang.String...))
