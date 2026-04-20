---
title: wsumBy
---

The `wsumBy` groups the data column according to `groupByColumns` and computes the weighted sum using `weightColumn` for the rest of the fields.

If the weight column is a floating point type, all result columns will be doubles. If the weight column is an integral type, all integral input columns will have long results and all floating point input columns will have double results.

## Syntax

```
table.wsumBy(weightColumn)
table.wsumBy(weightColumn, groupByColumns...)
table.wsumBy(weightColumn, groupByColumns)
```

## Parameters

<ParamTable>
<Param name="weightColumn" type="String">

The column to use as the weight for the calculation.

</Param>
<Param name="groupByColumns" type="String...">

The column(s) by which to group data.

- `NULL` returns the weighted sum for all non-key columns.
- `"X"` will output the weighted sum of each group in column `X`.
- `"X", "Y"` will output the weighted sum of each group designated from the `X` and `Y` columns.

</Param>
<Param name="groupByColumns" type="ColumnName...">

The column(s) by which to group data.

- `NULL` returns the weighted sum for all non-key columns.
- `"X"` will output the weighted sum of each group in column `X`.
- `"X", "Y"` will output the weighted sum of each group designated from the `X` and `Y` columns.

</Param>
<Param name="groupByColumns" type="Collection<String>">

The column(s) by which to group data.

- `NULL` returns the weighted sum for all non-key columns.
- `"X"` will output the weighted sum of each group in column `X`.
- `"X", "Y"` will output the weighted sum of each group designated from the `X` and `Y` columns.

</Param>
</ParamTable>

## Returns

A new table containing the weighted sum of the data column, grouped by the specified columns.

## Examples

In this example, `wsumBy` returns the weighted sum of the `Num1` column, with no grouping columns.

```groovy order=source,result
source = newTable(
    intCol("Num1", 1, 2, 3, 4, 5, 6, 7, 8, 9, 10),
    intCol("Num2", 5, 4, 3, 2, 1, 1, 1, 1, 1, 1)
)

result = source.wsumBy("Num2")
```

In this example, `wsumBy` returns the weighted sum of the `Num1` column, grouped by the `Letter` column.

```groovy order=source,result
source = newTable(
    stringCol("Letter", "A", "B", "A", "B", "A", "B", "A", "B", "A", "B"),
    intCol("Num1", 1, 2, 3, 4, 5, 6, 7, 8, 9, 10),
    intCol("Num2", 5, 4, 3, 2, 1, 1, 1, 1, 1, 1)
)

result = source.wsumBy("Num2", "Letter")
```

## Related documentation

- [How to create static tables](../../../how-to-guides/new-and-empty-table.md)
- [How to perform dedicated aggregations](../../../how-to-guides/dedicated-aggregations.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/api/TableOperations.html#wsumBy(java.lang.String))
