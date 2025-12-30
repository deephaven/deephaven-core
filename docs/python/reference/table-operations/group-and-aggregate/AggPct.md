---
title: pct
---

`agg.pct` returns an aggregator that computes the designated percentile of values, within an aggregation group, for each input column.

## Syntax

```
pct(percentile: float, cols: Union[str, list[str]], average_evenly_divided = False) -> Aggregation
```

## Parameters

<ParamTable>
<Param name="percentile" type="float">

The percentile to calculate.

</Param>
<Param name="cols" type="Union[str, list[str]]">

The source column(s) for the calculations.

- `["X"]` will output the designated percentile value in the `X` column for each group.
- `["Y = X"]` will output the designated percentile value in the `X` column for each group and rename it to `Y`.
- `["X, A = B"]` will output the designated percentile value in the `X` column for each group and the designated percentile value in the `B` value column renaming it to `A`.

</Param>
<Param name="average_evenly_divided" type="bool">

When the percentile splits the group into two halves, whether to average the two middle values for the output value.

- When set to `True`, the two middle values are averaged.
- When set to `False`, the smaller value is used. The default is `False`.
  This flag is only valid for numeric types.

</Param>
</ParamTable>

> [!CAUTION]
> If an aggregation does not rename the resulting column, the aggregation column will appear in the output table, not the input column. If multiple aggregations on the same column do not rename the resulting columns, an error will result, because the aggregations are trying to create multiple columns with the same name. For example, in `table.agg_by([agg.sum_(cols=[“X”]), agg.avg(cols=["X"])`, both the sum and the average aggregators produce column `X`, which results in an error.

## Returns

An aggregator that computes the designated percentile value, within an aggregation group, for each input column.

## Examples

In this example, `agg.pct` returns the 68th percentile value `Number` as grouped by `X`.

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

result = source.agg_by(
    [agg.pct(percentile=0.68, cols=["PctNumber = Number"])], by=["X"]
)
```

In this example, `agg.pct` returns the 68th percentile value `Number` and the 99th percentile value `Number` as grouped by `X`.

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

result = source.agg_by(
    [
        agg.pct(percentile=0.68, cols=["Pct68Number = Number"]),
        agg.pct(percentile=0.99, cols=["Pct99Number = Number"]),
    ],
    by=["X"],
)
```

In this example, `agg.pct` returns the 97th percentile value `Number`, and `agg.median` returns the median `Number`, as grouped by `X`. A second `result` table is then created to show the difference in output when `average_evenly_divided` is set to `True`.

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
    [
        agg.pct(percentile=0.97, cols=["Pct97Number = Number"]),
        agg.median(cols=["MedNumber = Number"]),
    ],
    by=["X"],
)
result = source.agg_by(
    [
        agg.pct(
            percentile=0.97, cols=["Pct97Number = Number"], average_evenly_divided=True
        ),
        agg.median(cols=["MedNumber = Number"]),
    ],
    by=["X"],
)
```

## Related documentation

- [How to create static tables](../../../how-to-guides/new-and-empty-table.md)
- [How to create multiple summary statistics for groups](../../../how-to-guides/combined-aggregations.md)
- [`agg_by`](./aggBy.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/api/agg/Aggregation.html#AggPct(double,java.lang.String...))
- [Pydoc](/core/pydoc/code/deephaven.agg.html#deephaven.agg.pct)
