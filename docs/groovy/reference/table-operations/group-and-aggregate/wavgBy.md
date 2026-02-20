---
title: wavgBy
---

The `wavgBy` method returns a new table with a column containing the weighted average of a table's data column, grouped by the specified columns.

## Syntax

```
table.wavgBy(weightColumn)
table.wavgBy(weightColumn, groupByColumns...)
table.wavgBy(weightColumn, groupByColumns)
```

## Parameters

<ParamTable>
<Param name="weightColumn" type="String">

The column to use as the weight for the calculation.

</Param>
<Param name="groupByColumns" type="String...">

The column(s) by which to group data.

- `NULL` returns the weighted average for all non-key columns.
- `"X"` will output the weighted average of each group in column `X`.
- `"X", "Y"` will output the weighted average of each group designated from the `X` and `Y` columns.

</Param>
<Param name="groupByColumns" type="ColumnName...">

The column(s) by which to group data.

- `NULL` returns the weighted average for all non-key columns.
- `"X"` will output the weighted average of each group in column `X`.
- `"X", "Y"` will output the weighted average of each group designated from the `X` and `Y` columns.

</Param>
<Param name="groupByColumns" type="Collection<String>">

The column(s) by which to group data.

- `NULL` returns the weighted average for all non-key columns.
- `"X"` will output the weighted average of each group in column `X`.
- `"X", "Y"` will output the weighted average of each group designated from the `X` and `Y` columns.

</Param>
</ParamTable>

## Returns

A new table with a column containing the weighted average of the data column, grouped by the specified columns.

## Examples

In this example, `wavgBy` returns the weighted average of the `Num1` column, with no grouping columns.

```groovy order=source,result
source = newTable(
    intCol("Num1", 1, 2, 3, 4, 5, 6, 7, 8, 9, 10),
    intCol("Num2", 5, 4, 3, 2, 1, 1, 1, 1, 1, 1)
)

result = source.wavgBy("Num2")
```

In this example, `wavgBy` returns the weighted average of the `Num1` column, grouped by the `Letter` column.

```groovy order=source,result
source = newTable(
    stringCol("Letter", "A", "B", "A", "B", "A", "B", "A", "B", "A", "B"),
    intCol("Num1", 1, 2, 3, 4, 5, 6, 7, 8, 9, 10),
    intCol("Num2", 5, 4, 3, 2, 1, 1, 1, 1, 1, 1)
)

result = source.wavgBy("Num2", "Letter")
```

## Related documentation

- [How to create static tables](../../../how-to-guides/new-and-empty-table.md)
- [How to perform dedicated aggregations](../../../how-to-guides/dedicated-aggregations.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/api/TableOperations.html#wavgBy(java.lang.String))
