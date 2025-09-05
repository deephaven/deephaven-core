---
title: "count_"
---

`agg.count_` returns an aggregator that computes the number of elements within an aggregation group.

> [!NOTE]
> `count` is a reserved Python keyword, so an underscore is used to maintain Python conventions.

## Syntax

```
count_(col: str) -> Aggregation
```

## Parameters

<ParamTable>
<Param name="col" type="str">

The name of the output column that will contain the number of elements in each group.

</Param>
</ParamTable>

## Returns

An aggregator that computes the number of elements within an aggregation group, for each group.

## Examples

In this example, `agg.count` adds a count column, `Number`, while `agg.avg`returns the average `Number`, as grouped by `X`.

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
    [agg.count_(col="Number"), agg.avg(cols="AvgNum = Number")], by=["X"]
)
```

## Related documentation

- [How to create static tables](../../../how-to-guides/new-and-empty-table.md)
- [How to create multiple summary statistics for groups](../../../how-to-guides/combined-aggregations.md)
- [`agg_by`](./aggBy.md)
- [Pydoc](/core/pydoc/code/deephaven.agg.html#deephaven.agg.count_)
