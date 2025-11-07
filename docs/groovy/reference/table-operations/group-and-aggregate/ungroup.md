---
title: ungroup
---

`ungroup` ungroups column content. It is the reverse of the [`groupBy`](./groupBy.md) method. `ungroup` expands columns containing either Deephaven arrays or Java arrays.

> [!CAUTION]
> Arrays of different lengths within a row result in an error.

## Syntax

```
table.ungroup()
table.ungroup(nullFill)
table.ungroup(nullFill, columnsToUngroup...)
table.ungroup(columnsToUngroup...)
table.ungroup(columnNames...)
```

## Parameters

<ParamTable>
<Param name="columnsToUngroup" type="String...">

The column(s) by which to ungroup data.

- `NULL` all array columns from the source table will be expanded.
- `"X"` will unwrap column `X`.
- `"X", "Y"` will unwrap columns `X` and `Y`.

</Param>
<Param name="columnsToUngroup" type="Collection<? extends ColumnName>">

The column(s) by which to ungroup data.

- `NULL` all array columns from the source table will be expanded.
- `"X"` will unwrap column `X`.
- `"X", "Y"` will unwrap columns `X` and `Y`.

</Param>
<Param name="nullFill" type="boolean">
Indicates if the ungrouped table should allow differently sized arrays and fill shorter columns with null values. If set to `false`, all arrays should be the same length.

</Param>
</ParamTable>

## Returns

A new table in which array columns from the source table are expanded into separate rows.

## Examples

In this example, `groupBy` returns an array of values for each column, as grouped by `X`. `ungroup` will undo this operation and all array columns from the source table are expanded into separate rows.

```groovy order=source,arrayTable,result
source = newTable(
    stringCol("X", "A", "B", "A", "C", "B", "A", "B", "B", "C"),
    stringCol("Y", "M", "N", "O", "N", "P", "M", "O", "P", "M"),
    intCol("Number", 55, 76, 20, 130, 230, 50, 73, 137, 214),
)

arrayTable = source.groupBy("X")

result = arrayTable.ungroup()
```

In this example, `groupBy` returns an array of values for each column, as grouped by `X`, while `ungroup` will expand the created array `Y` so each element is a new row.

```groovy order=source,arrayTable,result
source = newTable(
    stringCol("X", "A", "B", "A", "C", "B", "A", "B", "B", "C"),
    stringCol("Y", "M", "N", "O", "N", "P", "M", "O", "P", "M"),
    intCol("Number", 55, 76, 20, 130, 230, 50, 73, 137, 214),
)

arrayTable = source.groupBy("X")

result = arrayTable.ungroup("Y")
```

<!--TODO: https://github.com/deephaven/deephaven.io/issues/2460 add code examples -->

## Related documentation

- [Create a new table](../../../how-to-guides/new-and-empty-table.md#newtable)
- [How to group and ungroup data](../../../how-to-guides/grouping-data.md)
- [How to perform dedicated aggregations](../../../how-to-guides/dedicated-aggregations.md)
- [`groupBy``](./groupBy.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/api/TableOperations.html#ungroup())
