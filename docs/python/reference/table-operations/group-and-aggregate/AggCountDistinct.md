---
title: count_distinct
---

`agg.count_distinct` returns an aggregator that computes the number of distinct values, within an aggregation group, for each input column.

## Syntax

```python syntax
count_distinct(cols: Union[str, list[str]] = None, count_nulls: bool = False) -> Aggregation
```

## Parameters

<ParamTable>
<Param name="cols" type="Union[str, list[str]]">

The source column(s) for the calculations.

- `["X"]` will output the number of distinct values in the `X` column for each group.
- `["Y = X"]` will output the number of distinct values in the `X` column for each group and rename it to `Y`.
- `["X, A = B"]` will output the number of distinct values in the `X` column for each group and the number of distinct values in the `B` column while renaming it to `A`.

</Param>
<Param name="count_nulls" type="bool" optional>

Whether or not to count null values. Default is `False`.

</Param>
</ParamTable>

> [!CAUTION]
> If an aggregation does not rename the resulting column, the aggregation column will appear in the output table, not the input column. If multiple aggregations on the same column do not rename the resulting columns, an error will result, because the aggregations are trying to create multiple columns with the same name. For example, in `table.agg_by([agg.sum_(cols=[“X”]), agg.avg(cols=["X"])`, both the sum and the average aggregators produce column `X`, which results in an error.

## Returns

An aggregator that computes the number of distinct values, within an aggregation group, for each input column.

## Examples

In this example, `agg.count_distinct` returns the number of distinct values of `Y` as grouped by `X`

```python order=source,result
from deephaven import new_table
from deephaven.column import string_col, int_col, double_col
from deephaven import agg

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

result = source.agg_by([agg.count_distinct(cols=["Y"])], by=["X"])
```

In this example, `agg.count_distinct` returns the number of distinct values of `Y` (renamed to `Z`), as grouped by `X`.

```python order=source,result
from deephaven import new_table
from deephaven.column import string_col, int_col, double_col
from deephaven import agg

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

result = source.agg_by([agg.count_distinct(cols=["Z = Y"])], by=["X"])
```

In this example, `agg.count_distinct` returns the number of distinct values of `Y` and the number of distinct values of `Number`, as grouped by `X`.

```python order=source,result
from deephaven import new_table
from deephaven.column import string_col, int_col, double_col
from deephaven import agg

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

result = source.agg_by([agg.count_distinct(cols=["Y", "Number"])], by=["X"])
```

In this example, `agg.count_distinct` returns the number of distinct values of `Number`, as grouped by `X` and `Y`.

```python order=source,result
from deephaven import new_table
from deephaven.column import string_col, int_col, double_col
from deephaven import agg

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

result = source.agg_by([agg.count_distinct(cols=["Number"])], by=["X", "Y"])
```

In this example, `agg.count_distinct` returns the number of distinct values of `Number`, and `agg.last` returns the last `Number` integer, as grouped by `X`.

```python order=source,result
from deephaven import new_table
from deephaven.column import string_col, int_col, double_col
from deephaven import agg

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

result = source.agg_by(
    [
        agg.count_distinct(cols=["FirstNumber = Number"]),
        agg.last(cols=["LastNumber = Number"]),
    ],
    by=["X"],
)
```

This example demonstrates the effect of the `count_nulls` parameter.

```python order=source,result,result1
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
                NULL_INT,
                55,
                130,
                230,
                50,
                76,
                137,
                NULL_INT,
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

result = source.agg_by(
    [
        agg.count_distinct(cols=["FirstNumber = Number"], count_nulls=True),
        agg.last(cols=["LastNumber = Number"]),
    ],
    by=["X"],
)

result1 = source.agg_by(
    [
        agg.count_distinct(cols=["FirstNumber = Number"], count_nulls=False),
        agg.last(cols=["LastNumber = Number"]),
    ],
    by=["X"],
)
```

## Related documentation

- [How to create static tables](../../../how-to-guides/new-and-empty-table.md)
- [How to create multiple summary statistics for groups](../../../how-to-guides/combined-aggregations.md)
- [`agg_by`](./aggBy.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/api/agg/Aggregation.html#AggCountDistinct(java.lang.String...))
- [Pydoc](/core/pydoc/code/deephaven.agg.html#deephaven.agg.count_distinct)
