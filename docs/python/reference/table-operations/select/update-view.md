---
title: update_view
---

The `update_view` method creates a new table containing a new formula column for each argument.

When using `update_view`, the new columns are not stored in memory. Rather, a formula is stored that is used to recalculate each cell every time it is accessed.

> [!NOTE]
> The syntax for the `update_view`, [`update`](./update.md), and [`lazy_update`](./lazy-update.md) methods is identical, as is the resulting table. `update_view` is recommended when:
>
> 1. the formula is fast to compute,
> 2. only a small portion of the data is being accessed,
> 3. cells are accessed very few times, or
> 4. memory usage must be minimized.
>
> For other cases, consider using `select`, `view`, `update`, or `lazy_update`. These methods have different memory and computation expenses.

> [!CAUTION]
> When using [`view`](./view.md) or `update_view`, non-deterministic methods (e.g., random numbers, current time, or mutable structures) produce _unstable_ results. Downstream operations on these results produce _undefined_ behavior. Non-deterministic methods should use [`select`](./select.md) or [`update`](./update.md) instead.
> Concurrency control (serial marking and barriers) cannot be applied to [`view`](./view.md) or `update_view`. These operations compute results on demand and cannot enforce ordering constraints. If you need serial evaluation or barriers, use [`select`](./select.md) or [`update`](./update.md) instead. See [Parallelizing queries](../../../conceptual/query-engine/parallelization.md#serialization) for more information.

## Syntax

```
update_view(formulas: Union[str, Sequence[str]]) -> Table
```

## Parameters

<ParamTable>
<Param name="formulas" type="Union[str, Sequence[str]]">

Formulas to compute columns in the new table; e.g., `"X = A * sqrt(B)"`.

</Param>
</ParamTable>

## Returns

A new table that includes all the original columns from the source table and the newly defined formula columns.

## Examples

In the following example, a new table containing the columns from the source table plus two new columns, `X` and `Y`, is created.

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

result = source.update_view(formulas=["X = B", "Y = sqrt(C)"])
```

## Related documentation

- [Create a new table](../../../how-to-guides/new-and-empty-table.md#new_table)
- [How to select, view, and update data](../../../how-to-guides/use-select-view-update.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/api/TableOperations.html#updateView(java.lang.String...))
- [Pydoc](/core/pydoc/code/deephaven.table.html#deephaven.table.Table.update_view)
