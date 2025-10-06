---
title: agg_all_by
---

`agg_all_by` creates a new table containing grouping columns and grouped data. The resulting grouped data is defined by the aggregation specified.

> [!NOTE]
> Because `agg_all_by` applies the aggregation to all the columns of the table, it will ignore any column names specified for the aggregation.

## Syntax

```python syntax
agg_all_by(agg: Aggregation, by: Sequence[str] = None) -> Table
```

## Parameters

<ParamTable>
<Param name="agg" type="Aggregation">

The aggregation to apply to all columns of the source table.

</Param>
<Param name="by" type="Sequence[str]" optional>

The grouping column names. Set to `None` by default.

</Param>
</ParamTable>

## Returns

A new table containing grouping columns and grouped data.

## Examples

In this example, `agg_all_by` returns the median of `"Number"` values, grouped by `X`.

```python order=source,result
from deephaven import new_table
from deephaven.column import string_col, int_col, double_col
from deephaven import agg
from deephaven import table

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
                130,
                230,
                50,
                76,
                137,
                214,
                55,
                76,
                55,
                130,
                230,
                50,
                76,
                137,
                214,
            ],
        ),
    ]
)

result = source.agg_all_by(agg=agg.median(), by="X")
```

## Related documentation

- [How to create static tables](../../../how-to-guides/new-and-empty-table.md)
- [How to create multiple summary statistics for groups](../../../how-to-guides/combined-aggregations.md)
- [`agg_by`](./aggBy.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/api/agg/Aggregation.html#AggCountDistinct(java.lang.String...))
- [Pydoc](/core/pydoc/code/deephaven.agg.html#deephaven.agg.count_distinct)
