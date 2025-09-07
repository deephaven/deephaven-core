---
title: weighted_avg
---

`agg.weighted_avg` returns an aggregator that computes the weighted average of values, within an aggregation group, for each input column.

## Syntax

```
weighted_avg(wcol: str, cols: Union[str, list[str]]) -> Aggregation
```

## Parameters

<ParamTable>
<Param name="wcol" type="str">

The weight column for the calculation.

</Param>
<Param name="cols" type="Union[str, list[str]]">

The source column(s) for the calculations.

- `["X"]` will output the weighted average of values in the `X` column for each group.
- `["Y = X"]` will output the weighted average of values in the `X` column for each group and rename it to `Y`.
- `["X, A = B"]` will output the weighted average of values in the `X` column for each group and the weighted average of values in the `B` column and rename it to `A`.

</Param>

</ParamTable>

> [!CAUTION]
> If an aggregation does not rename the resulting column, the aggregation column will appear in the output table, not the input column. If multiple aggregations on the same column do not rename the resulting columns, an error will result, because the aggregations are trying to create multiple columns with the same name. For example, in `table.agg_by([agg.sum_(cols=[“X”]), agg.avg(cols=["X"])`, both the sum and the average aggregators produce column `X`, which results in an error.

## Returns

An aggregator that computes the weighted average of values, within an aggregation group, for each input column.

## Examples

In this example, `agg.weighted_avg` returns the weighted average of values of `Number`, as weighed by `Weight` and grouped by `X`.

```python order=source,result
from deephaven import new_table
from deephaven.column import string_col, int_col, double_col
from deephaven import agg as agg

source = new_table(
    [
        string_col("X", ["A", "B", "A", "C", "B", "A", "B", "B", "C"]),
        string_col("Y", ["M", "N", "O", "N", "P", "M", "O", "P", "M"]),
        int_col("Number", [55, 76, 20, 130, 230, 50, 73, 137, 214]),
        int_col("Weight", [1, 2, 1, 3, 2, 1, 4, 1, 2]),
    ]
)

result = source.agg_by([agg.weighted_avg(wcol="Weight", cols=["Number"])], by=["X"])
```

In this example, `agg.weighted_avg` returns the weighted average of values of `Number` (renamed to `WAvgNumber`), as weighed by `Weight` and grouped by `X`.

```python order=source,result
from deephaven import new_table
from deephaven.column import string_col, int_col, double_col
from deephaven import agg as agg

source = new_table(
    [
        string_col("X", ["A", "B", "A", "C", "B", "A", "B", "B", "C"]),
        string_col("Y", ["M", "N", "O", "N", "P", "M", "O", "P", "M"]),
        int_col("Number", [55, 76, 20, 130, 230, 50, 73, 137, 214]),
        int_col("Weight", [1, 2, 1, 3, 2, 1, 4, 1, 2]),
    ]
)

result = source.agg_by(
    [agg.weighted_avg(wcol="Weight", cols=["WAvgNumber = Number"])], by=["X"]
)
```

In this example, `agg.weighted_avg` returns the weighted average of values of `Number` (renamed to `WAvgNumber`), as weighed by `Weight` and grouped by `X` and `Y`.

```python order=source,result
from deephaven import new_table
from deephaven.column import string_col, int_col, double_col
from deephaven import agg as agg

source = new_table(
    [
        string_col("X", ["A", "B", "A", "C", "B", "A", "B", "B", "C"]),
        string_col("Y", ["M", "N", "O", "N", "P", "M", "O", "P", "M"]),
        int_col("Number", [55, 76, 20, 130, 230, 50, 73, 137, 214]),
        int_col("Weight", [1, 2, 1, 3, 2, 1, 4, 1, 2]),
    ]
)

result = source.agg_by(
    [agg.weighted_avg(wcol="Weight", cols=["WAvgNumber = Number"])], by=["X", "Y"]
)
```

In this example, `agg.weighted_avg` returns the weighted average of values of `Number1` (renamed to `WAvgNumber1`), and the weighted average of values of `Number2` (renamed to `WAvgNumber2`), both as weighed by `Weight` and grouped by `X`.

```python order=source,result
from deephaven import new_table
from deephaven.column import string_col, int_col, double_col
from deephaven import agg as agg

source = new_table(
    [
        string_col("X", ["A", "B", "A", "C", "B", "A", "B", "B", "C"]),
        int_col("Weight", [1, 2, 1, 3, 2, 1, 4, 1, 2]),
        int_col("Number1", [55, 76, 20, 130, 230, 50, 73, 137, 214]),
        int_col("Number2", [25, 14, 51, 23, 12, 15, 17, 72, 21]),
    ]
)

result = source.agg_by(
    [
        agg.weighted_avg(
            wcol="Weight", cols=["WAvgNumber1 = Number1", "WAvgNumber2 = Number2"]
        )
    ],
    by=["X"],
)
```

In this example, `agg.weighted_avg` returns the weighted average of values of `Number` (renamed to `WAvgNumber`), as weighed by `Weight` and grouped by `X`, and `agg.avg`returns the total average of values of `Number`, as grouped by `X`.

```python order=source,result
from deephaven import new_table
from deephaven.column import string_col, int_col, double_col
from deephaven import agg as agg

source = new_table(
    [
        string_col("X", ["A", "B", "A", "C", "B", "A", "B", "B", "C"]),
        string_col("Y", ["M", "N", "O", "N", "P", "M", "O", "P", "M"]),
        int_col("Number", [55, 76, 20, 130, 230, 50, 73, 137, 214]),
        int_col("Weight", [1, 2, 1, 3, 2, 1, 4, 1, 2]),
    ]
)

result = source.agg_by(
    [
        agg.weighted_avg(wcol="Weight", cols=["WAvgNumber = Number"]),
        agg.avg(cols=["Avg = Number"]),
    ],
    by=["X"],
)
```

## Related documentation

- [How to create static tables](../../../how-to-guides/new-and-empty-table.md)
- [How to create multiple summary statistics for groups](../../../how-to-guides/combined-aggregations.md)
- [`agg_by`](./aggBy.md)
- [`avg_by`](./avgBy.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/api/agg/Aggregation.html#AggWAvg(java.lang.String,java.lang.String...))
- [Pydoc](/core/pydoc/code/deephaven.agg.html#deephaven.agg.weighted_avg)
