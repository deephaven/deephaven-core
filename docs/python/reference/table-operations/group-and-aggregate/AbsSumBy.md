---
title: abs_sum_by
---

`abs_sum_by` creates a new table containing the absolute sum for each group.

## Syntax

```python syntax
table.abs_sum_by(by: Sequence[str] = None) -> Table
```

## Parameters

<ParamTable>
<Param name="by" type="Union[str, Sequence[str]]" optional>

The column(s) by which to group data. Default is `None`.

</Param>
</ParamTable>

## Returns

A new table containing the absolute sum for each group.

## Examples

In this example, `abs_sum_by` returns the absolute sum of the whole table.

```python order=source,result
from deephaven import new_table
from deephaven.column import string_col, int_col

source = new_table(
    [
        int_col("Number1", [100, -44, 49, 11, -66, 50, 29, 18, -70]),
        int_col("Number2", [-55, 76, 20, 130, 230, -50, 73, 137, 214]),
    ]
)

result = source.abs_sum_by()
```

In this example, `abs_sum_by` returns the absolute sum, as grouped by `X`. Because a sum cannot be computed for the string column `Y`, this column is dropped before applying `abs_sum_by`.

```python order=source,result
from deephaven import new_table
from deephaven.column import string_col, int_col

source = new_table(
    [
        string_col("X", ["A", "B", "A", "C", "B", "A", "B", "B", "C"]),
        string_col("Y", ["M", "N", "O", "N", "P", "M", "O", "P", "M"]),
        int_col("Number1", [100, -44, 49, 11, -66, 50, 29, 18, -70]),
        int_col("Number2", [-55, 76, 20, 130, 230, -50, 73, 137, 214]),
    ]
)

result = source.drop_columns(cols=["Y"]).abs_sum_by(by=["X"])
```

## Related documentation

- [How to create static tables](../../../how-to-guides/new-and-empty-table.md)
- [How to perform dedicated aggregations](../../../how-to-guides/dedicated-aggregations.md)
- [`agg_by`](./aggBy.md)
- [`drop_columns`](../select/drop-columns.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/api/TableOperations.html#absSumBy(io.deephaven.api.ColumnName...))
- [Pydoc](/core/pydoc/code/deephaven.table.html#deephaven.table.Table.abs_sum_by)
