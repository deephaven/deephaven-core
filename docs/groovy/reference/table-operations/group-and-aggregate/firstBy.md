---
title: firstBy
---

`firstBy` returns the first row for each group.

## Syntax

```
table.firstBy()
table.firstBy(groupByColumns...)
```

## Parameters

<ParamTable>
<Param name="groupByColumns" type="String...">

The column(s) by which to group data.

- `NULL` returns only the first row in the table.
- `"X"` will output the entire first row of each group in column `X`.
- `"X", "Y"` will output the entire first row of each group designated from the `X` and `Y` columns.

</Param>
<Param name="groupByColumns" type="ColumnName...">

The column(s) by which to group data.

- `NULL` returns only the first row in the table.
- `"X"` will output the entire first row of each group in column `X`.
- `"X", "Y"` will output the entire first row of each group designated from the `X` and `Y` columns.

</Param>
<Param name="groupByColumns" type="Collection<String>">

The column(s) by which to group data.

- `NULL` returns only the first row in the table.
- `"X"` will output the entire first row of each group in column `X`.
- `"X", "Y"` will output the entire first row of each group designated from the `X` and `Y` columns.

</Param>
</ParamTable>

## Returns

A new table containing the first row for each group.

## Examples

In this example, `firstBy` returns the first row of the table.

```groovy order=source,result
source = newTable(
    stringCol("X", "A", "B", "A", "C", "B", "A", "B", "B", "C"),
    stringCol("Y", "M", "N", "O", "N", "P", "M", "O", "P", "M"),
    intCol("Number", 55, 76, 20, 130, 230, 50, 73, 137, 214),
)

result = source.firstBy()
```

In this example, `firstBy` returns the first row, as grouped by `X`.

```groovy order=source,result
source = newTable(
    stringCol("X", "A", "B", "A", "C", "B", "A", "B", "B", "C"),
    stringCol("Y", "M", "N", "O", "N", "P", "M", "O", "P", "M"),
    intCol("Number", 55, 76, 20, 130, 230, 50, 73, 137, 214),
)

result = source.firstBy("X")
```

In this example, `firstBy` returns the first row, as grouped by `X` and `Y`.

```groovy order=source,result
source = newTable(
    stringCol("X", "A", "B", "A", "C", "B", "A", "B", "B", "C"),
    stringCol("Y", "M", "N", "O", "N", "P", "M", "O", "P", "M"),
    intCol("Number", 55, 76, 20, 130, 230, 50, 73, 137, 214),
)

result = source.firstBy("X", "Y")
```

## Related documentation

- [How to create static tables](../../../how-to-guides/new-and-empty-table.md)
- [How to perform dedicated aggregations](../../../how-to-guides/dedicated-aggregations.md)
- [`aggBy`](./aggBy.md)
- [`AggFirst`](./AggFirst.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/api/TableOperations.html#firstBy(java.lang.String...))
