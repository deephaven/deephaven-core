---
title: lazy_update
---

The `lazy_update` method creates a new table containing a new cached formula column for each argument.

When using `lazy_update`, cell values are stored in memory in a cache. Column formulas are computed on-demand and defer computation until it is required. Because it caches the results for the set of input values, the same input values will never be computed twice. Existing columns are referenced without additional memory allocation.

> [!NOTE]
> The syntax for the `lazy_update`, [`update_view`](./update-view.md), and [`update`](./update.md) methods is identical, as is the resulting table.
>
> `lazy_update` is recommended for small sets of unique input values. In this case, `lazy_update` uses less memory than `update` and requires less computation than `update_view`. However, if there are many unique input values, `update` will be more efficient because `lazy_update` stores the formula inputs and result in a map, whereas `update` stores the values more compactly in an array.

## Syntax

```
lazy_update(formulas: Union[str, Sequence[str]]) -> Table
```

## Parameters

<ParamTable>
<Param name="formulas" type="Union[str, Sequence[str]]">

Formulas to compute columns in the new table; e.g., `"X = A * sqrt(B)"`.

</Param>
</ParamTable>

## Returns

A new table that includes all the original columns from the source table and the newly defined columns.

## Examples

In the following example, a new table is created, containing the square root of column `C`. Because column `C` only contains the values 2 and 5, `sqrt(2)` is computed exactly one time, and `sqrt(5)` is computed exactly one time. The values are cached for future use.

```python order=source,result
from deephaven import new_table
from deephaven.column import string_col, int_col

source = new_table(
    [
        string_col("A", ["The", "At", "Is", "On"]),
        int_col("B", [1, 2, 3, 4]),
        int_col("C", [5, 2, 5, 5]),
    ]
)

result = source.lazy_update(formulas=["Y = sqrt(C)"])
```

## Related documentation

- [Create a new table](../../../how-to-guides/new-and-empty-table.md#new_table)
- [How to select, view, and update data](../../../how-to-guides/use-select-view-update.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/api/TableOperations.html#dropColumns(java.lang.String...))
- [Pydoc](/core/pydoc/code/deephaven.table.html#deephaven.table.Table.lazy_update)
