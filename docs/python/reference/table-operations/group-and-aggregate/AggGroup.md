---
title: group
---

`agg.group` returns an aggregator that computes an [array](../../query-language/types/arrays.md) of all values within an aggregation group, for each input column.

## Syntax

```
group(cols: Union[str, list[str]]) -> Aggregation
```

## Parameters

<ParamTable>
<Param name="cols" type="Union[str, list[str]]">

The source column(s) for the calculations.

- `["X"]` will output an array of all values in the `X` column for each group.
- `["Y = X"]` will output an [array](../../query-language/types/arrays.md) of all values in the `X` column for each group and rename it to `Y`.
- `["X, A = B"]` will output an [array](../../query-language/types/arrays.md) of all values in the `X` column for each group and an [array](../../query-language/types/arrays.md) of all values in the `B` value column renaming it to `A`.

</Param>
</ParamTable>

> [!CAUTION]
> If an aggregation does not rename the resulting column, the aggregation column will appear in the output table, not the input column. If multiple aggregations on the same column do not rename the resulting columns, an error will result, because the aggregations are trying to create multiple columns with the same name. For example, in `table.agg_by([agg.sum_(cols=[“X”]), agg.avg(cols=["X"])`, both the sum and the average aggregators produce column `X`, which results in an error.

## Returns

An aggregator that computes an [array](../../query-language/types/arrays.md) of all values, within an aggregation group, for each input column.

## Examples

In this example, `agg.group` returns an [array](../../query-language/types/arrays.md) of values of `Y` as grouped by `X`.

```python order=source,result
from deephaven import new_table
from deephaven.column import string_col, int_col, double_col
from deephaven import agg as agg

source = new_table(
    [
        string_col("X", ["A", "B", "A", "C", "B", "A", "B", "B", None]),
        string_col("Y", ["M", "N", None, "N", "P", "M", None, "P", "M"]),
        int_col("Number", [55, 76, 20, 130, 230, 50, 73, 137, 214]),
    ]
)

result = source.agg_by([agg.group(cols=["Y"])], by=["X"])
```

In this example, `agg.group` returns an [array](../../query-language/types/arrays.md) of values of `Y` (renamed to `Z`), as grouped by `X`.

```python order=source,result
from deephaven import new_table
from deephaven.column import string_col, int_col, double_col
from deephaven import agg as agg

source = new_table(
    [
        string_col("X", ["A", "B", "A", "C", "B", "A", "B", "B", None]),
        string_col("Y", ["M", "N", None, "N", "P", "M", None, "P", "M"]),
        int_col("Number", [55, 76, 20, 130, 230, 50, 73, 137, 214]),
    ]
)

result = source.agg_by([agg.group(cols=["Z = Y"])], by=["X"])
```

In this example, `agg.group` returns an [array](../../query-language/types/arrays.md) of values of `Y` (renamed to `Letters`), and an [array](../../query-language/types/arrays.md) of values of `Number` (renamed to `Numbers`), as grouped by `X`.

```python order=source,result
from deephaven import new_table
from deephaven.column import string_col, int_col, double_col
from deephaven import agg as agg

source = new_table(
    [
        string_col("X", ["A", "B", "A", "C", "B", "A", "B", "B", None]),
        string_col("Y", ["M", "N", None, "N", "P", "M", None, "P", "M"]),
        int_col("Number", [55, 76, 20, 130, 230, 50, 73, 137, 214]),
    ]
)
result = source.agg_by([agg.group(cols=["Letters = Y", "Numbers = Number"])], by=["X"])
```

In this example, `agg.group` returns an [array](../../query-language/types/arrays.md) of values (the `Number` column renamed to `Numbers`), as grouped by `X` and `Y`.

```python order=source,result
from deephaven import new_table
from deephaven.column import string_col, int_col, double_col
from deephaven import agg as agg

source = new_table(
    [
        string_col("X", ["A", "B", "A", "C", "B", "A", "B", "B", None]),
        string_col("Y", ["M", "N", None, "N", "P", "M", None, "P", "M"]),
        int_col("Number", [55, 76, 20, 130, 230, 50, 73, 137, 214]),
    ]
)

result = source.agg_by([agg.group(cols=["Numbers = Number"])], by=["X", "Y"])
```

In this example, `agg.group` returns an [array](../../query-language/types/arrays.md) of values `Number` (renamed to `Numbers`), and [`agg.max_`](./AggMax.md) returns the maximum `Number` integer, as grouped by `X`.

```python order=source,result
from deephaven import new_table
from deephaven.column import string_col, int_col, double_col
from deephaven import agg as agg

source = new_table(
    [
        string_col("X", ["A", "B", "A", "C", "B", "A", "B", "B", None]),
        string_col("Y", ["M", "N", None, "N", "P", "M", None, "P", "M"]),
        int_col("Number", [55, 76, 20, 130, 230, 50, 73, 137, 214]),
    ]
)
result = source.agg_by(
    [agg.group(cols=["Numbers = Number"]), agg.max_(cols=["MaxNumber = Number"])],
    by=["X"],
)
```

## Related documentation

- [How to create static tables](../../../how-to-guides/new-and-empty-table.md)
- [How to create multiple summary statistics for groups](../../../how-to-guides/combined-aggregations.md)
- [`agg.max\_`](./AggMax.md)
- [Arrays](../../query-language/types/arrays.md)
- [`agg_by`](./aggBy.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/api/agg/Aggregation.html#AggGroup(java.lang.String...))
- [Pydoc](/core/pydoc/code/deephaven.agg.html#deephaven.agg.group)
