---
title: tail_pct
---

The `tail_pct` method returns a table with a specific percentage of rows from the end of the source table.

> [!CAUTION]
> Attempting to use `tail_pct` on a [blink table](../../../conceptual/table-types.md#specialization-3-blink) will raise an error.

## Syntax

```python syntax
table.tail_pct(pct: float) -> Table
```

## Parameters

<ParamTable>
<Param name="pct" type="float">

The percentage of rows to return. This value must be given as a floating-point number between 0 (0%) to 1 (100%).

</Param>
</ParamTable>

## Returns

A new table with a specific percentage of rows from the end of the source table.

## Examples

The following example filters the table to the last 40% and 33.333333333% of rows.

```python order=source,result,result1
from deephaven import new_table
from deephaven.column import string_col, int_col, double_col
from deephaven.constants import NULL_INT

source = new_table(
    [
        string_col("Letter", ["A", "C", "F", "B", "E", "D", "A"]),
        int_col("Number", [NULL_INT, 2, 1, NULL_INT, 4, 5, 3]),
        string_col(
            "Color", ["red", "blue", "orange", "purple", "yellow", "pink", "blue"]
        ),
        int_col("Code", [12, 14, 11, NULL_INT, 16, 14, NULL_INT]),
    ]
)


result = source.tail_pct(pct=0.40)
result1 = source.tail_pct(pct=0.333333333)
```

## Related documentation

- [How to create static tables](../../../how-to-guides/new-and-empty-table.md)
- [How to use filters](../../../how-to-guides/use-filters.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/engine/table/Table.html#tailPct(double))
- [Pydoc](/core/pydoc/code/deephaven.table.html#deephaven.table.Table.tail_pct)
