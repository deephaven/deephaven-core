---
title: abs_sum
---

`abs_sum` returns an aggregator that computes an absolute sum, within an aggregation group, for each input column.

## Syntax

```
abs_sum(cols: list[str]) -> Aggregation
```

## Parameters

<ParamTable>
<Param name="cols" type="list[str]">

The source column(s) for the calculations.

- `["X"]` will output the total sum of values in the `X` column for each group.
- `["Y = X"]` will output the total sum of values in the `X` column for each group and rename it to `Y`.
- `["X, A = B"]` will output the total sum of values in the `X` column for each group and the total sum of values in the `B` value column renaming it to `A`.

</Param>
</ParamTable>

> [!CAUTION]
> If an aggregation does not rename the resulting column, the aggregation column will appear in the output table, not the input column. If multiple aggregations on the same column do not rename the resulting columns, an error will result, because the aggregations are trying to create multiple columns with the same name. For example, in `table.agg_by([agg.sum_(cols=[“X”]), agg.avg(cols=["X"])`, both the sum and the average aggregators produce column `X`, which results in an error.

## Returns

An aggregator that computes the absolute sum of values, within an aggregation group, for each input column.

## Examples

In this example, `agg.abs_sum` returns the sum of absolute values of the `X` (renamed to `AbsSumX`) column grouped by `Letter`.

```python order=source,result
from deephaven import empty_table
from deephaven import agg

source = empty_table(20).update(["X = i - 10", "Letter = (X % 2 == 0) ? `A` : `B`"])

result = source.agg_by([agg.abs_sum(cols=["AbsSumX = X"])], by=["Letter"])
```

In this example, `agg.abs_sum` returns the sum of absolute values of the `Number` column (renamed to `AbsSumNumber`), grouped by `X` and `Y`.

```python order=source,result
from deephaven import new_table
from deephaven.column import string_col, int_col, double_col
from deephaven import agg as agg

source = new_table(
    [
        string_col("X", ["A", "B", "A", "C", "B", "A", "B", "B", "C"]),
        string_col("Y", ["M", "P", "O", "N", "P", "M", "O", "P", "N"]),
        int_col("Number", [-55, -76, 20, -130, -230, 50, -73, -137, 214]),
    ]
)

result = source.agg_by([agg.abs_sum(cols=["AbsSumNumber = Number"])], by=["X", "Y"])
```

## Related documentation

- [How to create static tables](../../../how-to-guides/new-and-empty-table.md)
- [How to create multiple summary statistics for groups](../../../how-to-guides/combined-aggregations.md)
- [`agg_by`](./aggBy.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/api/agg/Aggregation.html#AggAbsSum(java.lang.String...))
- [Pydoc](/core/pydoc/code/deephaven.agg.html#deephaven.agg.abs_sum)
