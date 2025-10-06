---
title: avgBy
---

`avgBy` returns the average (mean) of each non-key column for each group. Null values are ignored.

> [!CAUTION]
> Applying this aggregation to a column where the average cannot be computed will result in an error. For example, the average is not defined for a column of string values.

## Syntax

```
table.avgBy()
table.avgBy(groupByColumns...)
```

## Parameters

<ParamTable>
<Param name="groupByColumns" type="String...">

The column(s) by which to group data.

- `NULL` returns total average for all non-key columns.
- `"X"` will output the total average of each group in column `X`.
- `"X", "Y"` will output the total average of each group designated from the `X` and `Y` columns.

</Param>
<Param name="groupByColumns" type="Collection<String>">

The column(s) by which to group data.

- `NULL` returns total average for all non-key columns.
- `"X"` will output the total average of each group in column `X`.
- `"X", "Y"` will output the total average of each group designated from the `X` and `Y` columns.

</Param>
<Param name="groupByColumns" type="ColumnName">

The column(s) by which to group data.

- `NULL` returns total average for all non-key columns.
- `"X"` will output the total average of each group in column `X`.
- `"X", "Y"` will output the total average of each group designated from the `X` and `Y` columns.

</Param>
</ParamTable>

## Returns

A new table containing the average for each group.

## Examples

In this example, `avgBy` returns the average value for the table. Because an average cannot be computed for the string columns `X` and `Y`, these columns are dropped before applying `avgBy`.

```groovy order=source,result
source = newTable(
    stringCol("X", "A", "B", "A", "C", "B", "A", "B", "B", "C"),
    stringCol("Y", "M", "N", "O", "N", "P", "M", "O", "P", "M"),
    intCol("Number", 55, 76, 20, 130, 230, 50, 73, 137, 214),
)

result = source.dropColumns("X", "Y").avgBy()
```

In this example, `avgBy` returns the average value, as grouped by `X`. Because an average cannot be computed for the string column `Y`, this column is dropped before applying `avgBy`.

```groovy order=source,result
source = newTable(
    stringCol("X", "A", "B", "A", "C", "B", "A", "B", "B", "C"),
    stringCol("Y", "M", "N", "O", "N", "P", "M", "O", "P", "M"),
    intCol("Number", 55, 76, 20, 130, 230, 50, 73, 137, 214),
)

result = source.dropColumns("Y").avgBy("X")
```

In this example, `avgBy` returns the average value, as grouped by `X` and `Y`.

```groovy order=source,result
source = newTable(
    stringCol("X", "A", "B", "A", "C", "B", "A", "B", "B", "C"),
    stringCol("Y", "M", "N", "O", "N", "P", "M", "O", "P", "M"),
    intCol("Number", 55, 76, 20, 130, 230, 50, 73, 137, 214),
)

result = source.avgBy("X", "Y")
```

## Related documentation

- [Create a new table](../../../how-to-guides/new-and-empty-table.md#newtable)
- [How to perform dedicated aggregations](../../../how-to-guides/dedicated-aggregations.md)
- [`aggBy`](./aggBy.md)
- [`dropColumns`](../select/drop-columns.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/api/TableOperations.html#avgBy(java.lang.String...))
