---
title: unique
---

`agg.unique` returns an aggregator that computes:

- the single unique value contained in each specified column,
- a customizable value, if there are no values present,
- or a customizable value, if there is more than one value present.

## Syntax

```python syntax
unique(
    cols: Union[str, list[str]],
    include_nulls: bool = False,
    non_unique_sentinel: Union[np.number, str, bool] = None
) -> Aggregation
```

<ParamTable>
<Param name="cols" type="Union[str, list[str]]">

The source column(s) to compute uniqueness for.

- `"X"` will output the single unique value in the `X` column for each group.
- `"Y = X"` will output the single unique value in the `X` column for each group and rename it to `Y`.
- `"X", "A = B"` will output the single unique value in the `X` column for each group and the single unique value in the `B` value column renaming it to `A`.

</Param>
<Param name="include_nulls" type="bool">

Whether nulls are counted as values. The default is `False`.

</Param>
<Param name="non_unique_sentinel" type="Union[np.number, str, bool]">

The non-null sentinel value when no unique value exists. The default is `None`. Must be a non-`None` value when `include_nulls` is `True`.

When passed in as a numpy scalar number value, it must be of one of these types: np.int8, np.int16, np.uint16, np.int32, np.int64(int), np.float32, np.float64(float). Please note that np.uint16 is interpreted as a Deephaven/Java char.

</Param>
</ParamTable>

> [!CAUTION]
> If an aggregation does not rename the resulting column, the aggregation column will appear in the output table, not the input column. If multiple aggregations on the same column do not rename the resulting columns, an error will result, because the aggregations are trying to create multiple columns with the same name. For example, in `table.agg_by([agg.sum_(cols=[“X”]), agg.avg(cols=["X"])`, both the sum and the average aggregators produce column `X`, which results in an error.

## Returns

An aggregator that computes the single unique value, within an aggregation group, for each input column. If there are no unique values or if there are multiple values, the aggregator returns a specified value.

## Examples

In the following example, `agg.unique` is used to find and return only the unique values of column `X`. Nulls are included.

```python order=source,result
from deephaven import new_table
from deephaven.column import string_col
from deephaven import agg as agg

n = "null"

source = new_table(
    [
        string_col("X", ["A", "A", "B", "B", "B", "C", "C", "D"]),
        string_col("Y", ["Q", "Q", "R", "R", n, "S", "T", n]),
    ]
)

result = source.agg_by(
    [agg.unique(cols="Y", include_nulls=True, non_unique_sentinel="#$#")], by=["X"]
)
```

In the following example, `agg.unique` is used to find and return only the unique values of column `X`.

<!--TODO: update example https://github.com/deephaven/deephaven-core/issues/1661 -->

```python order=source,result
from deephaven import new_table
from deephaven.column import string_col
from deephaven import agg as agg

source = new_table(
    [
        string_col("X", ["A", "A", "B", "B", "B", "C", "C", "D"]),
        string_col("Y", ["Q", "Q", "R", "R", None, "S", "T", None]),
    ]
)

result = source.agg_by([agg.unique(cols=["Y"])], by=["X"])
```

## Related documentation

- [How to create static tables](../../../how-to-guides/new-and-empty-table.md)
- [How to create multiple summary statistics for groups](../../../how-to-guides/combined-aggregations.md)
- [`agg_by`](./aggBy.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/api/agg/Aggregation.html#AggUnique(java.lang.String...))
- [Pydoc](/core/pydoc/code/deephaven.agg.html#deephaven.agg.unique)
