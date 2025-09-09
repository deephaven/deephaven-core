---
title: distinct
---

`agg.distinct` creates an aggregation that computes the distinct values within an aggregation group for each of the given columns and stores them as vectors.

## Syntax

```
distinct(cols: Union[str, list[str]], include_nulls = False) -> Aggregation
```

## Parameters

<ParamTable>
<Param name="cols" type="Union[str, list[str]]">

The source column(s) for the calculations.

- `["X"]` will output the number of distinct values in the `X` column for each group.
- `["Y = X"]` will output the number of distinct values in the `X` column for each group and rename it to `Y`.
- `["X, A = B"]` will output the number of distinct values in the `X` column for each group and the number of distinct values in the `B` column while renaming it to `A`.

</Param>
<Param name="include_nulls" type="bool">

Whether or not to include nulls as distinct values. Default is `False`.

</Param>
</ParamTable>

> [!CAUTION]
> If an aggregation does not rename the resulting column, the aggregation column will appear in the output table, not the input column. If multiple aggregations on the same column do not rename the resulting columns, an error will result, because the aggregations are trying to create multiple columns with the same name. For example, in `table.agg_by([agg.sum_(cols=[“X”]), agg.avg(cols=["X"])`, both the sum and the average aggregators produce column `X`, which results in an error.

## Returns

An aggregator that computes the number of distinct values within an aggregation group for each of the given columns and stores them as vectors.

## Examples

In this example, `agg.distinct` returns the number of distinct values of `Y` as grouped by `X`

```python order=source,result1,result2
from deephaven import new_table
from deephaven.column import string_col, int_col, double_col
from deephaven import agg
from deephaven.constants import NULL_INT

source = new_table(
    [
        string_col(
            "X",
            [
                "A",
                "B",
                "A",
                "C",
                "B",
                "A",
                "B",
                "B",
                "A",
                "A",
                "B",
                "A",
                "C",
                "B",
                "A",
                "B",
                "B",
                "C",
            ],
        ),
        string_col(
            "Y",
            [
                "M",
                "N",
                "M",
                "N",
                "N",
                "M",
                "O",
                "P",
                "N",
                "M",
                "N",
                "M",
                "N",
                "N",
                "M",
                "O",
                "P",
                "N",
            ],
        ),
        int_col(
            "Number",
            [
                55,
                76,
                55,
                NULL_INT,
                230,
                50,
                76,
                137,
                333,
                55,
                76,
                55,
                130,
                NULL_INT,
                50,
                76,
                137,
                214,
            ],
        ),
    ]
)

result1 = source.agg_by([agg.distinct(cols=["Number"])], by=["X"])
result2 = source.agg_by([agg.distinct(cols=["Number"], include_nulls=True)], by=["X"])
```

## Related documentation

- [How to create static tables](../../../how-to-guides/new-and-empty-table.md)
- [How to create multiple summary statistics for groups](../../../how-to-guides/combined-aggregations.md)
- [`agg_by`](./aggBy.md)
- [Javadoc](/core/javadoc/io/deephaven/api/agg/spec/AggSpecDistinct.html)
- [Pydoc](/core/pydoc/code/deephaven.agg.html#deephaven.agg.distinct)
