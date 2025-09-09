---
title: maxBy
---

`maxBy` returns the maximum value for each group. Null values are ignored.

## Syntax

```
table.maxBy()
table.maxBy(groupByColumns...)
```

## Parameters

<ParamTable>
<Param name="groupByColumns" type="String...">

The column(s) by which to group data.

- `NULL` returns the maximum value for each column in the table.
- `"X"` will output the maximum value of each group in column `X`.
- `"X", "Y"` will output the maximum value of each group designated from the `X` and `Y` columns.

</Param>
<Param name="groupByColumns" type="ColumnName...">

The column(s) by which to group data.

- `NULL` returns the maximum value for each column in the table.
- `"X"` will output the maximum value of each group in column `X`.
- `"X", "Y"` will output the maximum value of each group designated from the `X` and `Y` columns.

</Param>
<Param name="groupByColumns" type="Collection<String>">

The column(s) by which to group data.

- `NULL` returns the maximum value for each column in the table.
- `"X"` will output the maximum value of each group in column `X`.
- `"X", "Y"` will output the maximum value of each group designated from the `X` and `Y` columns.

</Param>
</ParamTable>

## Returns

A new table containing the maximum value for each group.

## Examples

In this example, `maxBy` returns the maximum value for each column.

```groovy order=source,result
source = newTable(
    stringCol("X", "A", "B", "A", "C", "B", "A", "B", "B", "C"),
    stringCol("Y", "M", "N", "O", "N", "P", "M", "O", "P", "M"),
    intCol("Number", 55, 76, 20, 130, 230, 50, 73, 137, 214),
)

result = source.maxBy()
```

In this example, `maxBy` returns the maximum value, as grouped by `X`.

```groovy order=source,result
source = newTable(
    stringCol("X", "A", "B", "A", "C", "B", "A", "B", "B", "C"),
    stringCol("Y", "M", "N", "O", "N", "P", "M", "O", "P", "M"),
    intCol("Number", 55, 76, 20, 130, 230, 50, 73, 137, 214),
)

result = source.maxBy("X")
```

In this example, `maxBy` returns the maximum value, as grouped by `X` and `Y`.

```groovy order=source,result
source = newTable(
    stringCol("X", "A", "B", "A", "C", "B", "A", "B", "B", "C"),
    stringCol("Y", "M", "N", "O", "N", "P", "M", "O", "P", "M"),
    intCol("Number", 55, 76, 20, 130, 230, 50, 73, 137, 214),
)

result = source.maxBy("X", "Y")
```

## Related documentation

- [Create a new table](../../../how-to-guides/new-and-empty-table.md#newtable)
- [How to perform dedicated aggregations](../../../how-to-guides/dedicated-aggregations.md)
- [`aggBy`](./aggBy.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/api/TableOperations.html#maxBy(java.lang.String...))
