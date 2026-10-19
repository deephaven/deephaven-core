---
title: update_view
---

The `update_view` method creates a new table containing a new formula column for each argument.

A formula column is [not stored in memory](../../../how-to-guides/use-select-view-update.md#do-you-want-to-write-new-columns-to-memory-or-calculate-them-on-demand). Instead, the table stores the column's formula and recalculates each cell every time the cell is accessed.

> [!NOTE]
> `update_view` and [`lazy_update`](./lazy-update.md) accept only [formula strings](../../../how-to-guides/formulas.md). [`update`](./update.md) accepts formula strings and [`Selectable`](../../query-language/types/Selectable.md) objects.
>
> `update_view` fits best when:
>
> 1. the formula is fast to compute,
> 2. only a small portion of the data is being accessed,
> 3. cells are accessed very few times, or
> 4. memory usage must be minimized.
>
> [`select`](./select.md), [`view`](./view.md), [`update`](./update.md), and [`lazy_update`](./lazy-update.md) differ from `update_view` in memory and computation costs.

> [!CAUTION]
> When using [`view`](./view.md) or `update_view`, non-deterministic methods (e.g., random numbers, current time, or mutable structures) produce _unstable_ results. Downstream operations on these results produce _undefined_ behavior. Non-deterministic methods belong in [`select`](./select.md) or [`update`](./update.md) instead.

> [!CAUTION]
> [`view`](./view.md) and `update_view` compute cells when a reader accesses them, on any thread and in any order, so they cannot guarantee [evaluation order](../../../conceptual/query-engine/parallelization.md#serial-execution). A formula that must run in order belongs in [`select`](./select.md) or [`update`](./update.md) instead.

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

In the following example, `update_view` creates a table that contains the columns from the source table plus two new columns, `X` and `Y`. The result looks the same as the result of [`update`](./update.md). The column `X = B` refers to the data of column `B`, so it needs no calculation. `Y = sqrt(C)` is a formula column: `update_view` stores no values for it and recalculates it when a reader accesses a cell.

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

- [Choose the right selection method for your query](../../../how-to-guides/use-select-view-update.md#choose-the-right-column-selection-method)
- [Create a new table](../../../how-to-guides/new-and-empty-table.md#new_table)
- [Formulas](../../../how-to-guides/formulas.md)
- [How to select, view, and update data](../../../how-to-guides/use-select-view-update.md)
- [Parallelization](../../../conceptual/query-engine/parallelization.md)
- [Selectable](../../query-language/types/Selectable.md)
- [select](./select.md)
- [view](./view.md)
- [update](./update.md)
- [lazy_update](./lazy-update.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/api/TableOperations.html#updateView(java.lang.String...))
- [Pydoc](/core/pydoc/code/deephaven.table.html#deephaven.table.Table.update_view)
