---
title: var_by
---

`var_by` returns the sample variance for each group. Null values are ignored.

Sample variance is calculated using the [Bessel correction](https://en.wikipedia.org/wiki/Bessel%27s_correction), which ensures it is an [unbiased estimator](https://en.wikipedia.org/wiki/Bias_of_an_estimator) of population variance under some conditions.

> [!CAUTION]
> Applying this aggregation to a column where the sample variance can not be computed will result in an error. For example, the sample variance is not defined for a column of string values.

## Syntax

```
table.var_by(by: Union[str, list[str]]) -> Table
```

## Parameters

<ParamTable>
<Param name="by" type="Union[str, list[str]]" optional>

The column(s) by which to group data.

- `[]` returns the sample variance for all non-key columns (default).
- `"X"` will output the sample variance of each group in column `X`.
- `"X", "Y"` will output the sample variance of each group designated from the `X` and `Y` columns.

</Param>
</ParamTable>

## Returns

A new table containing the sample variance for each group.

## How to calculate sample variance

Sample variance is a measure of the average dispersion of data values from the mean. Unlike sample standard deviation, it is not on the same scale as the data, meaning that sample variance cannot be readily interpreted in the same units as the data. The formula for sample variance is as follows:

$$
s = \frac{\sum_{i=1}^{n}{(x_i - \bar{x})^2}}{n-1}
$$

## Examples

In this example, `var_by` returns the sample variance of the whole table. Because the sample variance can not be computed for the string columns `X` and `Y`, these columns are dropped before applying `var_by`.

```python order=source,result
from deephaven import new_table
from deephaven.column import string_col, int_col

source = new_table(
    [
        string_col("X", ["A", "B", "A", "C", "B", "A", "B", "B", "C"]),
        string_col("Y", ["M", "N", "O", "N", "P", "M", "O", "P", "M"]),
        int_col("Number", [55, 76, 20, 130, 230, 50, 73, 137, 214]),
    ]
)

result = source.drop_columns(cols=["X", "Y"]).var_by()
```

In this example, `var_by` returns the sample variance, as grouped by `X`. Because the sample variance can not be computed for the string column `Y`, this column is dropped before applying `var_by`.

```python order=source,result
from deephaven import new_table
from deephaven.column import string_col, int_col

source = new_table(
    [
        string_col("X", ["A", "B", "A", "C", "B", "A", "B", "B", "C"]),
        string_col("Y", ["M", "N", "O", "N", "P", "M", "O", "P", "M"]),
        int_col("Number", [55, 76, 20, 130, 230, 50, 73, 137, 214]),
    ]
)

result = source.drop_columns(cols=["Y"]).var_by(by=["X"])
```

In this example, `var_by` returns the sample variance, as grouped by `X` and `Y`.

```python order=source,result
from deephaven import new_table
from deephaven.column import string_col, int_col

source = new_table(
    [
        string_col("X", ["A", "B", "A", "C", "B", "A", "B", "B", "C"]),
        string_col("Y", ["M", "N", "O", "N", "P", "M", "O", "P", "M"]),
        int_col("Number", [55, 76, 20, 130, 230, 50, 73, 137, 214]),
    ]
)

result = source.var_by(by=["X", "Y"])
```

## Related documentation

- [How to create static tables](../../../how-to-guides/new-and-empty-table.md)
- [How to perform dedicated aggregations](../../../how-to-guides/dedicated-aggregations.md)
- [`agg_by`](./aggBy.md)
- [`var`](./AggVar.md)
- [`drop_columns`](../select/drop-columns.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/api/TableOperations.html#varBy(java.lang.String...))
- [Pydoc](/core/pydoc/code/deephaven.table.html#deephaven.table.Table.var_by)
