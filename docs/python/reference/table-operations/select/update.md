---
title: update
---

The `update` method creates a new table containing a new, in-memory column for each argument.

When using `update`, the new columns are evaluated and stored in memory. Existing columns are referenced without additional memory allocation.

> [!NOTE]
> The syntax for the `update`, [`update_view`](./update-view.md), and [`lazy_update`](./lazy-update.md) methods is identical, as is the resulting table. `update` is recommended when:
>
> 1. all the source columns are desired in the result,
> 2. the formula is expensive to evaluate,
> 3. cells are accessed many times, and/or
> 4. a large amount of memory is available.
>
> When memory usage or computation needs to be reduced, consider using `select`, `view`, `update_view`, or `lazy_update`. These methods have different memory and computation expenses.

## Syntax

```
update(formulas: Union[str, Sequence[str]]) -> Table
```

## Parameters

<ParamTable>
<Param name="formulas" type="Union[str, Sequence[str]]">

Formulas to compute columns in the new table; e.g., `"X = A * sqrt(B)"`.

</Param>
</ParamTable>

## Returns

A new table that includes all the original columns from the source table and the newly defined in-memory columns.

## Examples

In the following example, the new columns (`A`, `X`, and `Y`) allocate memory and are immediately populated with values. Columns `B` and `C` refer to columns in the source table and do not allocate memory.

```python order=source,result
from deephaven import new_table
from deephaven.column import string_col, int_col

source = new_table(
    [
        string_col("A", ["The", "At", "Is", "On"]),
        int_col("B", [1, 2, 3, 4]),
        int_col("C", [5, 6, 7, 8]),
    ]
)

result = source.update(formulas=["A", "X = B", "Y = sqrt(C)"])
```

## Related documentation

- [Create a new table](../../../how-to-guides/new-and-empty-table.md#new_table)
- [How to select, view, and update data](../../../how-to-guides/use-select-view-update.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/api/TableOperations.html#update(java.lang.String...))
- [Pydoc](/core/pydoc/code/deephaven.table.html#deephaven.table.Table.update)
