---
title: sorted_last
---

`agg.sorted_last` returns an aggregator that sorts a table in descending order and then computes the first value, within an aggregation group, for each input column.

> [!NOTE]
> `agg.sorted_last` will produce the same results as a [`sort`](../sort/sort.md) operation followed by [`agg.last`](./AggLast.md).

## Syntax

```
sorted_last(order_by: str, cols: Union[str, list[str]]) -> Aggregation
```

## Parameters

<ParamTable>
<Param name="order_by" type="str">

The column to sort by.

</Param>
<Param name="cols" type="Union[str, list[str]]">

The source column(s) for the calculations.

- `["X"]` will output the last value in the `X` column for each group.
- `["Y = X"]` will output the last value in the `X` column for each group and rename it to `Y`.
- `["X, A = B"]` will output the last value in the `X` column for each group and the last value in the `B` column while renaming it to `A`.

</Param>
</ParamTable>

> [!CAUTION]
> If an aggregation does not rename the resulting column, the aggregation column will appear in the output table, not the input column. If multiple aggregations on the same column do not rename the resulting columns, an error will result, because the aggregations are trying to create multiple columns with the same name. For example, in `table.agg_by([agg.sum_(cols=[“X”]), agg.avg(cols=["X"])`, both the sum and the average aggregators produce column `X`, which results in an error.

## Returns

An aggregator that sorts the table in descending order and then computes the first value, within an aggregation group, for each input column.

## Examples

In this example, `agg.sorted_last` returns the last `Y` value as sorted by `Z` and grouped by `X`.

```python order=source,result
from deephaven import new_table
from deephaven.column import string_col, int_col, double_col
from deephaven import agg as agg

source = new_table(
    [
        string_col("X", ["A", "B", "A", "C", "B", "A", "B", "B", "C"]),
        string_col("Y", ["N", "O", "P", "N", "P", "N", "", "Q", "O"]),
        int_col("Z", [3, 2, 1, 1, 3, 1, 4, 1, 2]),
    ]
)

result = source.agg_by([agg.sorted_last(order_by="Z", cols=["Y"])], by=["X"])
```

In this example, `agg.sorted_last` returns the last `Y` value (renamed to `Z`), as sorted by `Z` and grouped by `X`.

```python order=source,result
from deephaven import new_table
from deephaven.column import string_col, int_col, double_col
from deephaven import agg as agg

source = new_table(
    [
        string_col("X", ["A", "B", "A", "C", "B", "A", "B", "B", "C"]),
        string_col("Y", ["N", "O", "P", "N", "P", "N", "", "Q", "O"]),
        int_col("Z", [3, 2, 1, 1, 3, 1, 4, 1, 2]),
    ]
)

result = source.agg_by([agg.sorted_last(order_by="Z", cols=["Z = Y"])], by=["X"])
```

In this example, `agg.sorted_last` returns the last `Y` string and last `Z` integer, as sorted by `Z` and grouped by `X`.

```python order=source,result
from deephaven import new_table
from deephaven.column import string_col, int_col, double_col
from deephaven import agg as agg

source = new_table(
    [
        string_col("X", ["A", "B", "A", "C", "B", "A", "B", "B", "C"]),
        string_col("Y", ["N", "O", "P", "N", "P", "N", "", "Q", "O"]),
        int_col("Z", [3, 2, 1, 1, 3, 1, 4, 1, 2]),
    ]
)

result = source.agg_by([agg.sorted_last(order_by="Z", cols=["Y", "Z"])], by=["X"])
```

In this example, `agg.sorted_last` returns the last `Z` integer, as sorted by `Z` and grouped by `X` and `Y`.

```python order=source,result
from deephaven import new_table
from deephaven.column import string_col, int_col, double_col
from deephaven import agg as agg

source = new_table(
    [
        string_col("X", ["A", "B", "A", "C", "B", "A", "B", "B", "C"]),
        string_col("Y", ["N", "O", "P", "N", "P", "N", "", "Q", "O"]),
        int_col("Z", [3, 2, 1, 1, 3, 1, 4, 1, 2]),
    ]
)

result = source.agg_by([agg.sorted_last(order_by="Z", cols=["Z"])], by=["X", "Y"])
```

In this example, `agg.sorted_last` returns the last `Y` string, and [`agg.max_`](./AggMax.md) returns the maximum `Z` integer, as sorted by `Z` and grouped by `X`.

```python order=source,result
from deephaven import new_table
from deephaven.column import string_col, int_col, double_col
from deephaven import agg as agg

source = new_table(
    [
        string_col("X", ["A", "B", "A", "C", "B", "A", "B", "B", "C"]),
        string_col("Y", ["N", "O", "P", "N", "P", "N", "", "Q", "O"]),
        int_col("Z", [3, 2, 1, 1, 3, 1, 4, 1, 2]),
    ]
)

result = source.agg_by(
    [agg.sorted_last(order_by="Z", cols=["SortedLastY = Y"]), agg.max_("Z")], by=["X"]
)
```

## Related documentation

- [How to create static tables](../../../how-to-guides/new-and-empty-table.md)
- [`agg_by`](./aggBy.md)
- [`sort`](../sort/sort.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/api/agg/Aggregation.html#AggSortedLast(java.lang.String,java.lang.String...))
- [Pydoc](/core/pydoc/code/deephaven.agg.html#deephaven.agg.sorted_last)
