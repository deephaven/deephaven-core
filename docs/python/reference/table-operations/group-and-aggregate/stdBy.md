---
title: std_by
---

`std_by` returns the sample standard deviation for each group. Null values are ignored.

Sample standard deviation is calculated as the square root of the [Bessel-corrected sample variance](https://en.wikipedia.org/wiki/Bessel%27s_correction), which can be shown to be an [unbiased estimator](https://en.wikipedia.org/wiki/Bias_of_an_estimator) of population variance under some conditions. However, sample standard deviation is a biased estimator of population standard deviation.

> [!CAUTION]
> Applying this aggregation to a column where the sample standard deviation cannot be computed will result in an error. For example, the sample standard deviation is not defined for a column of string values.

## Syntax

```
table.std_by(by: Union[str, list[str]]) -> Table
```

## Parameters

<ParamTable>
<Param name="by" type="Union[str, list[str]]" optional>

The column(s) by which to group data.

- `[]` returns the sample standard deviation for all non-key columns (default).
- `["X"]` will output the sample standard deviation of each group in column `X`.
- `["X", "Y"]` will output the sample standard deviation of each group designated from the `X` and `Y` columns.

</Param>
</ParamTable>

## Returns

A new table containing the sample standard deviation for each group.

## How to calculate sample standard deviation

Sample standard deviation is a measure of the average dispersion of data values from the mean. Unlike sample variance, it is on the same scale as the data, meaning that sample standard deviation can be readily interpreted in the same units as the data. The formula for sample standard deviation is as follows:

$$
s = \sqrt{\frac{\sum_{i=1}^{n}{(x_i - \bar{x})^2}}{n-1}}
$$

## Examples

In this example, `std_by` returns the sample standard deviation of the whole table. Because the sample standard deviation cannot be computed for the string columns `X` and `Y`, these columns are dropped before applying `std_by`.

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

result = source.drop_columns(cols=["X", "Y"]).std_by()
```

In this example, `std_by` returns the sample standard deviation, as grouped by `X`. Because the sample standard deviation cannot be computed for the string column `Y`, this column is dropped before applying `std_by`.

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

result = source.drop_columns(cols=["Y"]).std_by(by=["X"])
```

In this example, `std_by` returns the sample standard deviation, as grouped by `X` and `Y`.

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

result = source.std_by(by=["X", "Y"])
```

## Related documentation

- [How to create static tables](../../../how-to-guides/new-and-empty-table.md)
- [How to perform dedicated aggregations](../../../how-to-guides/dedicated-aggregations.md)
- [`agg_by`](./aggBy.md)
- [`agg.std`](./AggStd.md)
- [`drop_columns`](../select/drop-columns.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/api/TableOperations.html#stdBy(java.lang.String...))
- [Pydoc](/core/pydoc/code/deephaven.table.html#deephaven.table.Table.std_by)
