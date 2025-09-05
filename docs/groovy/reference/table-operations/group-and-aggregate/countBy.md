---
title: countBy
---

`countBy` returns the number of rows for each group.

## Syntax

```
table.countBy(countColumnName)
table.countBy(countColumnName, groupByColumns...)
```

## Parameters

<ParamTable>
<Param name="countColumnName" type="String">

The name of the output column containing the count.

</Param>
<Param name="groupByColumns" type="String...">

The column(s) by which to group data.

- `NULL` returns the total count of rows in the table.
- `"X"` will output the count of each group in column `X`.
- `"X", "Y"` will output the count of each group designated from the `X` and `Y` columns.

</Param>
<Param name="groupByColumns" type="ColumnName...">

The column(s) by which to group data.

- `NULL` returns the total count of rows in the table.
- `"X"` will output the count of each group in column `X`.
- `"X", "Y"` will output the count of each group designated from the `X` and `Y` columns.

</Param>
<Param name="groupByColumns" type="Collection<String>">

The column(s) by which to group data.

- `NULL` returns the total count of rows in the table.
- `"X"` will output the count of each group in column `X`.
- `"X", "Y"` will output the count of each group designated from the `X` and `Y` columns.

</Param>
</ParamTable>

## Returns

A new table containing the number of rows for each group.

## Examples

In this example, `countBy` returns the number of rows in the table and stores that in a new column, `Count`.

```groovy order=source,result
source = newTable(
    stringCol("X", "A", "B", "A", "C", "B", "A", "B", "B", "C"),
    stringCol("Y", "M", "N", "O", "N", "P", "M", "O", "P", "M"),
    intCol("Number", 55, 76, 20, 130, 230, 50, 73, 137, 214),
)

result = source.countBy("Count")
```

In this example, `countBy` returns the number of rows in the table as grouped by `X` and stores that in a new column, `Count`.

```groovy order=source,result
source = newTable(
    stringCol("X", "A", "B", "A", "C", "B", "A", "B", "B", "C"),
    stringCol("Y", "M", "N", "O", "N", "P", "M", "O", "P", "M"),
    intCol("Number", 55, 76, 20, 130, 230, 50, 73, 137, 214),
)

result = source.countBy("Count", "X")
```

In this example, `countBy` returns the number of rows in the table as grouped by `X` and `Y`, and stores that in a new column `Count`.

```groovy order=source,result
source = newTable(
    stringCol("X", "A", "B", "A", "C", "B", "A", "B", "B", "C"),
    stringCol("Y", "M", "N", "O", "N", "P", "M", "O", "P", "M"),
    intCol("Number", 55, 76, 20, 130, 230, 50, 73, 137, 214),
)

result = source.countBy("Count", "X", "Y")
```

## Related documentation

- [Create a new table](../../../how-to-guides/new-and-empty-table.md#newtable)
- [How to perform dedicated aggregations](../../../how-to-guides/dedicated-aggregations.md)
- [`aggBy`](./aggBy.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/api/TableOperations.html#countBy(java.lang.String))
