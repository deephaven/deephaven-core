---
title: groupBy
---

`groupBy` groups column content into arrays.

## Syntax

```
table.groupBy()
table.groupBy(groupByColumns...)
```

## Parameters

<ParamTable>
<Param name="groupByColumns" type="String...">

The column(s) by which to group data.

- `NULL` the content of each column is grouped into its own array.
- `"X"` will output an array of values for each group in column `X`.
- `"X", "Y"` will output an array of values for each group designated from the `X` and `Y` columns.

</Param>
<Param name="groupByColumns" type="Collection<? extends ColumnName>">

The column(s) by which to group data.

- `NULL` the content of each column is grouped into its own array.
- `"X"` will output an array of values for each group in column `X`.
- `"X", "Y"` will output an array of values for each group designated from the `X` and `Y` columns.

</Param>
</ParamTable>

## Returns

A new table containing grouping columns and grouped data. Column content is grouped into arrays.

## Examples

In this example, `groupBy` creates an array of values for each column.

```groovy order=source,result
source = newTable(
    stringCol("X", "A", "B", "A", "C", "B", "A", "B", "B", "C"),
    stringCol("Y", "M", "N", "O", "N", "P", "M", "O", "P", "M"),
    intCol("Number", 55, 76, 20, 130, 230, 50, 73, 137, 214),
)

result = source.groupBy()
```

In this example, `groupBy` creates an array of values, as grouped by `X`.

```groovy order=source,result
source = newTable(
    stringCol("X", "A", "B", "A", "C", "B", "A", "B", "B", "C"),
    stringCol("Y", "M", "N", "O", "N", "P", "M", "O", "P", "M"),
    intCol("Number", 55, 76, 20, 130, 230, 50, 73, 137, 214),
)

result = source.groupBy("X")
```

In this example, `groupBy` creates an array of values, as grouped by `X` and `Y`.

```groovy order=source,result
source = newTable(
    stringCol("X", "A", "B", "A", "C", "B", "A", "B", "B", "C"),
    stringCol("Y", "M", "N", "O", "N", "P", "M", "O", "P", "M"),
    intCol("Number", 55, 76, 20, 130, 230, 50, 73, 137, 214),
)

result = source.groupBy("X", "Y")
```

## Related documentation

- [Create a new table](../../../how-to-guides/new-and-empty-table.md#newtable)
- [How to create multiple summary statistics for groups](../../../how-to-guides/combined-aggregations.md)
- [How to group and ungroup data](../../../how-to-guides/grouping-data.md)
- [`aggBy`](./aggBy.md)
- [`partitionBy`](./partitionBy.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/api/TableOperations.html#groupBy(java.lang.String...))
