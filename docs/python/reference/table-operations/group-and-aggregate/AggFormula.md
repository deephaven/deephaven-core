---
title: formula
---

`formula` returns an aggregator that computes a user-defined formula aggregation across specified columns.

## Syntax

```syntax
formula(formula: str, formula_param: Optional[str] = None, cols: List[str]) -> Aggregation
```

## Parameters

<ParamTable>
<Param name="formula" type="str">

The user-defined [formula](../../../how-to-guides/formulas.md) to apply to each group. The [formula](../../../how-to-guides/formulas.md) can contain a combination of any of the following:

- Built-in functions such as `min`, `max`, etc.
- Mathematical arithmetic such as `*`, `+`, `/`, etc.
- [Python functions](../../../how-to-guides/python-functions.md)

If `formula_param` is not `None`, the formula can only be applied to one column at a time, and it is applied to the specified `formula_param`. If `formula_param` is `None`, the formula is applied to any column or literal value present in the formula. The use of `formula_param` is deprecated.

Key column(s) can be used as input to the formula. When this happens, key values are treated as scalars.

</Param>
<Param name="formula_param" type="str">

The parameter name within the formula. If `formula_param` is `each`, then `formula` must contain `each`. For example, `max(each)`, `min(each)`, etc. Use of this parameter is deprecated.

</Param>
<Param name="cols" type="list[str]">

The source column(s) for the calculations.

- `["X"]` applies the formula to each value in the `X` column for each group.
- `["Y = X"]` applies the formula to each value in the `X` column for each group and renames it to `Y`.
- `["X, A = B"]` applies the formula to each value in the `X` column for each group and the formula to each value in the `B` column while renaming it to `A`.

</Param>
</ParamTable>

> [!CAUTION]
> If an aggregation does not rename the resulting column, the aggregation column will appear in the output table, not the input column. If multiple aggregations on the same column do not rename the resulting columns, an error will result, because the aggregations are trying to create multiple columns with the same name. For example, in `table.agg_by([agg.sum_(cols=[“X”]), agg.avg(cols=["X"])`, both the sum and the average aggregators produce column `X`, which results in a name conflict error.

## Returns

An aggregator that computes a user-defined formula within an aggregation group, for each input column.

## Examples

The following example uses `agg.formula` to calculate several formula aggregations by the `Letter` column. Since `formula_param` is `None`, the formulas are applied to any column or literal value present in the formula. The specified formulas operate on zero, one, two, and three different columns at a time.

```python order=source,result
from deephaven import empty_table
from deephaven import agg

source = empty_table(20).update(
    ["X = i", "Y = 2 * i", "Z = 3 * i", "Letter = (X % 2 == 0) ? `A` : `B`"]
)

result = source.agg_by(
    [
        agg.formula(formula="OutA = sqrt(5.0)"),
        agg.formula(formula="OutB = min(X)"),
        agg.formula(formula="OutC = min(X) + max(Y)"),
        agg.formula(formula="OutD = sum(X + Y + Z)"),
    ],
    by=["Letter"],
)
```

In this example, `agg.formula` is used to calculate the aggregate minimum across several columns by the `Letter` column. The example uses `formula_param` to tell Deephaven that the formula applies to `each`.

```python order=source,result
from deephaven import empty_table
from deephaven import agg

source = empty_table(20).update(
    ["X = i", "Y = 2 * i", "Z = 3 * i", "Letter = (X % 2 == 0) ? `A` : `B`"]
)

result = source.agg_by(
    agg.formula(
        formula="min(each)",
        formula_param="each",
        cols=["MinX = X", "MinY = Y", "MinZ = Z"],
    ),
    by=["Letter"],
)
```

In this example, `agg.formula` is used to calculate the aggregated average across several columns by the `Letter` and `Color` column. Since the formula is applied to one column at a time, the `each` parameter is used to refer to the current column being processed.

```python order=source,result
from deephaven import empty_table
from deephaven import agg

colors = ["Red", "Blue", "Green"]

formulas = [
    "X = 0.1 * i",
    "Y1 = Math.pow(X, 2)",
    "Y2 = Math.sin(X)",
    "Y3 = Math.cos(X)",
]
grouping_cols = ["Letter = (i % 2 == 0) ? `A` : `B`", "Color = (String)colors[i % 3]"]

source = empty_table(40).update(formulas + grouping_cols)

my_agg = [
    agg.formula(
        formula="avg(k)",
        formula_param="k",
        cols=[f"AvgY{idx} = Y{idx}" for idx in range(1, 4)],
    )
]

result = source.agg_by(aggs=my_agg, by=["Letter", "Color"])
```

In this example, `agg.formula` is used to calculate the aggregate sum of squares across each of the `X`, `Y`, and `Z` columns by the `Letter` column. Note that since the formula is applied to one column at a time, the `each` parameter is used to refer to the current column being processed.

```python order=source,result
from deephaven import empty_table
from deephaven import agg

source = empty_table(20).update(
    ["X = i", "Y = 2 * i", "Z = 3 * i", "Letter = (X % 2 == 0) ? `A` : `B`"]
)

my_agg = [
    agg.formula(
        formula="sum(each * each)",
        formula_param="each",
        cols=["SumSqrX = X", "SumSqrY = Y", "SumSqrZ = Z"],
    )
]

result = source.agg_by(my_agg, by=["Letter"])
```

In this example, `agg.formula` calls a user-defined function, `range`, to calculate the aggregate range of the `X`, `Y`, and `Z` columns by `Letter`. Note that since the formula is applied to one column at a time, the `each` parameter is used to refer to the current column being processed.

> [!CAUTION]
> Type hints will not set the column type when used in `agg.formula`. Use an explicit typecast in the formula string to ensure the resultant column(s) are of the correct type.

```python order=source,result
from deephaven import empty_table
from deephaven import agg


def range(each):
    return max(each) - min(each)


source = empty_table(20).update(
    ["X = i", "Y = 2 * i", "Z = 3 * i", "Letter = (X % 2 == 0) ? `A` : `B`"]
)

result = source.agg_by(
    [
        agg.formula(
            formula="(int)range(each)",
            formula_param="each",
            cols=["RangeX = X", "RangeY = Y", "RangeZ = Z"],
        )
    ],
    by=["Letter"],
)
```

## Related documentation

- [How to create static tables](../../../how-to-guides/new-and-empty-table.md)
- [How to create multiple summary statistics for groups](../../../how-to-guides/combined-aggregations.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/api/agg/Aggregation.html#AggFormula(java.lang.String,java.lang.String,java.lang.String...))
- [Pydoc](/core/pydoc/code/deephaven.agg.html#deephaven.agg.formula)
