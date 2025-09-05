---
title: absSumBy
---

The `absSumBy` method groups the data column according to `groupByColumns` and computes the sum of the absolute values for the rest of the fields.

## Syntax

```
table.absSumBy()
table.absSumBy(groupByColumns...)
```

## Parameters

<ParamTable>
<Param name="groupByColumns" type="String...">

The column(s) by which to group data.

- `NULL` returns the absolute sum for all non-key columns.
- `"X"` will output the absolute sum of each group in column `X`.
- `"X", "Y"` will output the absolute sum of each group designated from the `X` and `Y` columns.

</Param>
<Param name="groupByColumns" type="ColumnName...">

The column(s) by which to group data.

- `NULL` returns the absolute sum for all non-key columns.
- `"X"` will output the absolute sum of each group in column `X`.
- `"X", "Y"` will output the absolute sum of each group designated from the `X` and `Y` columns.

</Param>
<Param name="groupByColumns" type="Collection<String>">

The column(s) by which to group data.

- `NULL` returns the absolute sum for all non-key columns.
- `"X"` will output the absolute sum of each group in column `X`.
- `"X", "Y"` will output the absolute sum of each group designated from the `X` and `Y` columns.

</Param>
</ParamTable>

## Returns

A new table containing the absolute sum for each group.

## Examples

In this example, `absSumBy` returns the absolute sum of the whole table. Because the absolute sum cannot be computed for the string columns `X` and `Y`, these columns are dropped before applying `absSumBy`.

```groovy order=source,result
source = newTable(
    stringCol("X", "A", "B", "A", "C", "B", "A", "B", "B", "C"),
    stringCol("Y", "M", "N", "O", "N", "P", "M", "O", "P", "M"),
    intCol("Number", 55, 76, 20, -130, 230, -50, 73, 137, -214),
)

result = source.dropColumns("X", "Y").absSumBy()
```

In this example, `absSumBy` returns the absolute sum as grouped by `X`. Because the absolute sum cannot be computed for the string column `Y`, this column is dropped before applying `absSumBy`.

```groovy order=source,result
source = newTable(
    stringCol("X", "A", "B", "A", "C", "B", "A", "B", "B", "C"),
    stringCol("Y", "M", "N", "O", "N", "P", "M", "O", "P", "M"),
    intCol("Number", 55, 76, 20, -130, 230, -50, 73, 137, -214),
)

result = source.dropColumns("Y").absSumBy("X")
```

In this example, `absSumBy` returns the absolute sum as grouped by both `X` and `Y`.

```groovy order=source,result
source = newTable(
    stringCol("X", "A", "B", "A", "C", "B", "A", "B", "B", "C"),
    stringCol("Y", "M", "N", "O", "N", "P", "M", "O", "P", "M"),
    intCol("Number", 55, 76, 20, -130, 230, -50, 73, 137, -214),
)

result = source.absSumBy("X", "Y")
```

## Related documentation

- [Create a new table](../../../how-to-guides/new-and-empty-table.md#newtable)
- [How to perform dedicated aggregations](../../../how-to-guides/dedicated-aggregations.md)
- [`aggBy`](./aggBy.md)
- [`dropColumns`](../select/drop-columns.md)
- [Javadoc](https://deephaven.io//core/javadoc/io/deephaven/api/TableOperations.html#absSumBy(java.lang.String...))
