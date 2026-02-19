---
title: stdBy
---

`stdBy` returns the standard deviation for each group. Null values are ignored.

> [!CAUTION]
> Applying this aggregation to a column where the standard deviation cannot be computed will result in an error. For example, the standard deviation is not defined for a column of string values.

## Syntax

```
table.stdBy()
table.stdBy(groupByColumns...)
```

## Parameters

<ParamTable>
<Param name="groupByColumns" type="String...">

The column(s) by which to group data.

- `NULL` returns the standard deviation for all non-key columns.
- `"X"` will output the standard deviation of each group in column `X`.
- `"X", "Y"` will output the standard deviation of each group designated from the `X` and `Y` columns.

</Param>
<Param name="groupByColumns" type="ColumnName...">

The column(s) by which to group data.

- `NULL` returns the standard deviation for all non-key columns.
- `"X"` will output the standard deviation of each group in column `X`.
- `"X", "Y"` will output the standard deviation of each group designated from the `X` and `Y` columns.

</Param>
<Param name="groupByColumns" type="Collection<String>">

The column(s) by which to group data.

- `NULL` returns the standard deviation for all non-key columns.
- `"X"` will output the standard deviation of each group in column `X`.
- `"X", "Y"` will output the standard deviation of each group designated from the `X` and `Y` columns.

</Param>
</ParamTable>

## Returns

A new table containing the standard deviation for each group.

## How to calculate standard deviation

Standard deviation is a measure of the dispersion of data values from the mean. The formula for standard deviation is the square root of the sum of squared differences from the mean divided by the size of the data set. For example:

$$
s = \sqrt{\frac{\sum_{i=1}^{n}{(x_i - \bar{x})^2}}{n-1}}
$$

## Examples

In this example, `stdBy` returns the standard deviation of the whole table. Because the standard deviation cannot be computed for the string columns `X` and `Y`, these columns are dropped before applying `stdBy`.

```groovy order=source,result
source = newTable(
    stringCol("X", "A", "B", "A", "C", "B", "A", "B", "B", "C"),
    stringCol("Y", "M", "N", "O", "N", "P", "M", "O", "P", "M"),
    intCol("Number", 55, 76, 20, 130, 230, 50, 73, 137, 214),
)

result = source.dropColumns("X", "Y").stdBy()
```

In this example, `stdBy` returns the standard deviation, as grouped by `X`. Because the standard deviation cannot be computed for the string column `Y`, this column is dropped before applying `stdBy`.

```groovy order=source,result
source = newTable(
    stringCol("X", "A", "B", "A", "C", "B", "A", "B", "B", "C"),
    stringCol("Y", "M", "N", "O", "N", "P", "M", "O", "P", "M"),
    intCol("Number", 55, 76, 20, 130, 230, 50, 73, 137, 214),
)

result = source.dropColumns("Y").stdBy("X")
```

In this example, `stdBy` returns the standard deviation, as grouped by `X` and `Y`.

```groovy order=source,result
source = newTable(
    stringCol("X", "A", "B", "A", "C", "B", "A", "B", "B", "C"),
    stringCol("Y", "M", "N", "O", "N", "P", "M", "O", "P", "M"),
    intCol("Number", 55, 76, 20, 130, 230, 50, 73, 137, 214),
)

result = source.stdBy("X", "Y")
```

## Related documentation

- [How to create static tables](../../../how-to-guides/new-and-empty-table.md)
- [How to perform dedicated aggregations](../../../how-to-guides/dedicated-aggregations.md)
- [`aggBy`](./aggBy.md)
- [`dropColumns`](../select/drop-columns.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/api/TableOperations.html#stdBy(java.lang.String...))
