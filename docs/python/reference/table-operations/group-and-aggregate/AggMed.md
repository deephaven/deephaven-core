---
title: median
---

`agg.median` returns an aggregator that computes the median value, within an aggregation group, for each input column.

## Syntax

```
median(cols: Union[str, list[str]], average_evenly_divided = True) -> Aggregation
```

## Parameters

<ParamTable>
<Param name="cols" type="Union[str, list[str]]">

The source column(s) for the calculations.

- `["X"]` will output the median value in the `X` column for each group.
- `["Y = X"]` will output the median value in the `X` column for each group and rename it to `Y`.
- `["X, A = B"]` will output the median value in the `X` column for each group and the median value in the `B` column while renaming it to `A`.

</Param>
<Param name="average_evenly_divided" type="bool">

When the group size is an even number, whether to average the two middle values for the output value.

- When set to `True`, the two middle values are averaged. The default is `True`.
- When set to `False`, the smaller value is used.\
  This flag is only valid for numeric types.

</Param>
</ParamTable>

> [!CAUTION]
> If an aggregation does not rename the resulting column, the aggregation column will appear in the output table, not the input column. If multiple aggregations on the same column do not rename the resulting columns, an error will result, because the aggregations are trying to create multiple columns with the same name. For example, in `table.agg_by([agg.sum_(cols=[“X”]), agg.avg(cols=["X"])`, both the sum and the average aggregators produce column `X`, which results in an error.

## Returns

An aggregator that computes the median value, within an aggregation group, for each input column.

## Examples

In this example, `agg.median` returns the median `Number` value as grouped by `X`.

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

result = source.agg_by([agg.median(cols=["Number"])], by=["X"])
```

In this example, `agg.median` returns the median `Number` value (renamed to `Z`), as grouped by `X`.

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

result = source.agg_by([agg.median(cols=["Z = Number"])], by=["X"])
```

In this example, `agg.median` returns the median `Number`, as grouped by `X` and `Y`.

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

result = source.agg_by([agg.median(cols=["Number"])], by=["X", "Y"])
```

In this example, `agg.median` returns the median `Number`, and `agg.max_` returns the maximum `Number`, as grouped by `X`.

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
    [agg.median(cols=["MedNumber = Number"]), agg.max_(cols=["MaxNumber = Number"])],
    by=["X"],
)
```

This example demonstrates the effect of the `average_evenly_divided` parameter. The first table returns the median `Number` with `average_evenly_divided` set to `False`; the second table shows the default behavior.

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
    [agg.median(cols=["MedNumber = Number"], average_evenly_divided=False)], by=["X"]
)
result1 = source.agg_by([agg.median(cols=["MedNumber = Number"])], by=["X"])
```

## Related documentation

- [How to create static tables](../../../how-to-guides/new-and-empty-table.md)
- [How to create multiple summary statistics for groups](../../../how-to-guides/combined-aggregations.md)
- [`agg_by`](./aggBy.md)
- [`median_by`](./medianBy.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/api/agg/Aggregation.html#AggMed(java.lang.String...))
- [Pydoc](/core/pydoc/code/deephaven.agg.html#deephaven.agg.median)
