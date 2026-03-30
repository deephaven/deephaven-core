---
title: medianBy
---

`medianBy` returns the median value for each group. Null values are ignored.

## Syntax

```
table.medianBy()
table.medianBy(groupByColumns...)
```

## Parameters

<ParamTable>
<Param name="groupByColumns" type="String...">

The column(s) by which to group data.

- `NULL` returns the median value for each column in the table.
- `"X"` will output the median value of each group in column `X`.
- `"X", "Y"` will output the median value of each group designated from the `X` and `Y` columns.

</Param>
<Param name="groupByColumns" type="ColumnName...">

The column(s) by which to group data.

- `NULL` returns the median value for each column in the table.
- `"X"` will output the median value of each group in column `X`.
- `"X", "Y"` will output the median value of each group designated from the `X` and `Y` columns.

</Param>
<Param name="groupByColumns" type="Collection<String>">

The column(s) by which to group data.

- `NULL` returns the median value for each column in the table.
- `"X"` will output the median value of each group in column `X`.
- `"X", "Y"` will output the median value of each group designated from the `X` and `Y` columns.

</Param>
</ParamTable>

## Returns

A new table containing the median value for each group.

## Examples

In this example, `medianBy` returns the median value for each column.

```groovy order=source,result
source = newTable(
    stringCol("X", "A", "B", "A", "C", "B", "A", "B", "B", "C"),
    stringCol("Y", "M", "N", "O", "N", "P", "M", "O", "P", "M"),
    intCol("Number", 55, 76, 20, 130, 230, 50, 73, 137, 214),
)

result = source.medianBy()
```

In this example, `medianBy` returns the median value, as grouped by `X`.

```groovy order=source,result
source = newTable(
    stringCol("X", "A", "B", "A", "C", "B", "A", "B", "B", "C"),
    stringCol("Y", "M", "N", "O", "N", "P", "M", "O", "P", "M"),
    intCol("Number", 55, 76, 20, 130, 230, 50, 73, 137, 214),
)

result = source.medianBy("X")
```

In this example, `medianBy` returns the median value, as grouped by `X` and `Y`.

```groovy order=source,result
source = newTable(
    stringCol("X", "A", "B", "A", "C", "B", "A", "B", "B", "C"),
    stringCol("Y", "M", "N", "O", "N", "P", "M", "O", "P", "M"),
    intCol("Number", 55, 76, 20, 130, 230, 50, 73, 137, 214),
)

result = source.medianBy("X", "Y")
```

## Related documentation

- [How to create static tables](../../../how-to-guides/new-and-empty-table.md)
- [How to perform dedicated aggregations](../../../how-to-guides/dedicated-aggregations.md)
- [`aggBy`](./aggBy.md)
- [`AggMed`](./AggMed.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/api/TableOperations.html#medianBy(java.lang.String...))
