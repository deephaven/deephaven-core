---
title: std
---

`agg.std` returns an aggregator that computes the sample standard deviation of values, within an aggregation group, for each input column.

Sample standard deviation is calculated as the square root of the [Bessel-corrected sample variance](https://en.wikipedia.org/wiki/Bessel%27s_correction), which can be shown to be an [unbiased estimator](https://en.wikipedia.org/wiki/Bias_of_an_estimator) of population variance under some conditions. However, sample standard deviation is a biased estimator of population standard deviation.

## Syntax

```
std(cols: Union[str, list[str]]) -> Aggregation
```

## Parameters

<ParamTable>
<Param name="cols" type="Union[str, list[str]]">

The source column(s) for the calculations.

- `["X"]` will output the sample standard deviation of values in the `X` column for each group.
- `["Y = X"]` will output the sample standard deviation of values in the `X` column for each group and rename it to `Y`.
- `["X, A = B"]` will output the sample standard deviation of values in the `X` column for each group and the sample standard deviation of values in the `B` value column renaming it to `A`.

</Param>
</ParamTable>

> [!CAUTION]
> If an aggregation does not rename the resulting column, the aggregation column will appear in the output table, not the input column. If multiple aggregations on the same column do not rename the resulting columns, an error will result, because the aggregations are trying to create multiple columns with the same name. For example, in `table.agg_by([agg.sum_(cols=[“X”]), agg.avg(cols=["X"])`, both the sum and the average aggregators produce column `X`, which results in an error.

## Returns

An aggregator that computes the sample standard deviation of values, within an aggregation group, for each input column.

## Examples

In this example, `agg.std` returns the sample standard deviation of values of `Number` as grouped by `X`.

```python order=source,result
from deephaven import new_table
from deephaven.column import string_col, int_col, double_col
from deephaven import agg as agg

source = new_table(
    [
        string_col("X", ["A", "B", "A", "C", "B", "A", "B", "B", "C"]),
        string_col("Y", ["M", "N", "O", "N", "P", "M", "O", "P", "M"]),
        int_col("Number", [55, 76, 20, 130, 230, 50, 73, 137, 214]),
    ]
)

result = source.agg_by([agg.std(cols=["Number"])], by=["X"])
```

In this example, `agg.std` returns the sample standard deviation of values of `Number` (renamed to `Std`), as grouped by `X`.

```python order=source,result
from deephaven import new_table
from deephaven.column import string_col, int_col, double_col
from deephaven import agg as agg

source = new_table(
    [
        string_col("X", ["A", "B", "A", "C", "B", "A", "B", "B", "C"]),
        string_col("Y", ["M", "N", "O", "N", "P", "M", "O", "P", "M"]),
        int_col("Number", [55, 76, 20, 130, 230, 50, 73, 137, 214]),
    ]
)

result = source.agg_by([agg.std(cols=["Std = Number"])], by=["X"])
```

In this example, `agg.std` returns the sample standard deviation of values of `Number` (renamed to `Std`), as grouped by `X` and `Y`.

```python order=source,result
from deephaven import new_table
from deephaven.column import string_col, int_col, double_col
from deephaven import agg as agg

source = new_table(
    [
        string_col("X", ["A", "B", "A", "C", "B", "A", "B", "B", "C"]),
        string_col("Y", ["M", "N", "O", "N", "P", "M", "O", "P", "M"]),
        int_col("Number", [55, 76, 20, 130, 230, 50, 73, 137, 214]),
    ]
)
result = source.agg_by([agg.std(cols=["Std = Number"])], by=["X", "Y"])
```

In this example, `agg.first` returns the sample standard deviation of values of `Number`, and `agg.median` returns the median value of `Number`, as grouped by `X`.

```python order=source,result
from deephaven import new_table
from deephaven.column import string_col, int_col, double_col
from deephaven import agg as agg

source = new_table(
    [
        string_col("X", ["A", "B", "A", "C", "B", "A", "B", "B", "C"]),
        string_col("Y", ["M", "P", "O", "N", "P", "M", "O", "P", "N"]),
        int_col("Number", [55, 76, 20, 130, 230, 50, 73, 137, 214]),
    ]
)

result = source.agg_by(
    [agg.std(cols=["Std = Number"]), agg.median(cols=["Median = Number"])], by=["X"]
)
```

## Related documentation

- [How to create static tables](../../../how-to-guides/new-and-empty-table.md)
- [How to create multiple summary statistics for groups](../../../how-to-guides/combined-aggregations.md)
- [`agg_by`](./aggBy.md)
- [`std_by`](./stdBy.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/api/agg/Aggregation.html#AggStd(java.lang.String...))
- [Pydoc](/core/pydoc/code/deephaven.agg.html#deephaven.agg.std)
