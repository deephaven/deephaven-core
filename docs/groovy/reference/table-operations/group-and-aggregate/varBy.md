---
title: varBy
---

`varBy` returns the variance for each group. Null values are ignored.

> [!CAUTION]
> Applying this aggregation to a column where the variance cannot be computed will result in an error. For example, the variance is not defined for a column of string values.

## Syntax

```
table.varBy()
table.varBy(groupByColumns...)
```

## Parameters

<ParamTable>
<Param name="groupByColumns" type="String...">

The column(s) by which to group data.

- `NULL` returns the variance for all non-key columns.
- `"X"` will output the variance of each group in column `X`.
- `"X", "Y"` will output the variance of each group designated from the `X` and `Y` columns.

</Param>
<Param name="groupByColumns" type="ColumnName...">

The column(s) by which to group data.

- `NULL` returns the variance for all non-key columns.
- `"X"` will output the variance of each group in column `X`.
- `"X", "Y"` will output the variance of each group designated from the `X` and `Y` columns.

</Param>
<Param name="groupByColumns" type="Collection<String>">

The column(s) by which to group data.

- `NULL` returns the variance for all non-key columns.
- `"X"` will output the variance of each group in column `X`.
- `"X", "Y"` will output the variance of each group designated from the `X` and `Y` columns.

</Param>
</ParamTable>

## Returns

A new table containing the variance for each group.

## How to calculate variance

1. Find the mean of the data set. Add all data values and divide by the sample size $n$.

$$
\bar{x} = \frac{\sum_{i=1}^{n}{x_i}}{n}
$$

2. Find the squared difference from the mean for each data value. Subtract the mean from each data value and square the result.

$$
(x_i - \bar{x})^2
$$

3. Find the sum of all the squared differences. The sum of squares is all the squared differences added together.

$$
SS = \sum_{i=1}^{n}{(x_i - \bar{x})^2}
$$

4. Calculate the variance. Variance is the sum of squares divided by the number of data points. The formula for variance for a sample set of data is:

$$
s^2 = \frac{\Sigma (x_i - \bar{x})^2 }{n-1}
$$

## Examples

In this example, `varBy` returns the variance of the whole table. Because the variance cannot be computed for the string columns `X` and `Y`, these columns are dropped before applying `varBy`.

```groovy order=source,result
source = newTable(
    stringCol("X", "A", "B", "A", "C", "B", "A", "B", "B", "C"),
    stringCol("Y", "M", "N", "O", "N", "P", "M", "O", "P", "M"),
    intCol("Number", 55, 76, 20, 130, 230, 50, 73, 137, 214),
)

result = source.dropColumns("X", "Y").varBy()
```

In this example, `varBy` returns the variance, as grouped by `X`. Because the variance cannot be computed for the string column `Y`, this column is dropped before applying `varBy`.

```groovy order=source,result
source = newTable(
    stringCol("X", "A", "B", "A", "C", "B", "A", "B", "B", "C"),
    stringCol("Y", "M", "N", "O", "N", "P", "M", "O", "P", "M"),
    intCol("Number", 55, 76, 20, 130, 230, 50, 73, 137, 214),
)

result = source.dropColumns("Y").varBy("X")
```

In this example, `varBy` returns the variance, as grouped by `X` and `Y`.

```groovy order=source,result
source = newTable(
    stringCol("X", "A", "B", "A", "C", "B", "A", "B", "B", "C"),
    stringCol("Y", "M", "N", "O", "N", "P", "M", "O", "P", "M"),
    intCol("Number", 55, 76, 20, 130, 230, 50, 73, 137, 214),
)

result = source.varBy("X", "Y")
```

## Related documentation

- [How to create static tables](../../../how-to-guides/new-and-empty-table.md)
- [How to perform dedicated aggregations](../../../how-to-guides/dedicated-aggregations.md)
- [`aggBy`](./aggBy.md)
- [`dropColumns`](../select/drop-columns.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/api/TableOperations.html#varBy(java.lang.String...))
