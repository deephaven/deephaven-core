---
title: view
---

The `view` method creates a new table that includes one formula column for each argument.

A formula column is not stored in memory. Instead, the table stores the column's formula and recalculates each cell every time the cell is accessed.

> [!NOTE]
> `view` accepts only [formula strings](../../../how-to-guides/formulas.md). [`select`](./select.md) accepts [formula strings](../../../how-to-guides/formulas.md) and [`Selectable`](../../query-language/types/Selectable.md) objects. Both methods return a table with one column for each argument. `view` stores formulas and recalculates cells on access, while [`select`](./select.md) stores those columns in memory.
>
> `view` fits best when:
>
> 1. the formula is fast to compute,
> 2. only a small portion of the data is being accessed,
> 3. cells are accessed very few times, or
> 4. memory usage must be minimized.
>
> [`select`](./select.md), [`update_view`](./update-view.md), [`update`](./update.md), and [`lazy_update`](./lazy-update.md) differ from `view` in memory and computation costs.

> [!CAUTION]
> When using `view` or [`update_view`](./update-view.md), non-deterministic methods (e.g., random numbers, current time, or mutable structures) produce _unstable_ results. Downstream operations on these results produce _undefined_ behavior. Non-deterministic methods belong in [`select`](./select.md) or [`update`](./update.md) instead.

> [!CAUTION]
> `view` and [`update_view`](./update-view.md) do not support [serial execution](../../../conceptual/query-engine/parallelization.md#serial-execution) or respecting [Barriers](../../../conceptual/query-engine/parallelization.md#barriers). These operations compute results lazily, when cells are accessed, so rows can be evaluated in any order, at any time, and by any thread. This makes ordering guarantees meaningless. A formula that requires serial evaluation or must wait for another operation belongs in [`select`](./select.md) or [`update`](./update.md) instead.

## Syntax

```
view(formulas: Union[str, Sequence[str]]) -> Table
```

## Parameters

<ParamTable>
<Param name="formulas" type="Union[str, Sequence[str]]">

Formulas to compute columns in the new table:

- Column from `table`: `"A"` (equivalent to `"A = A"`)
- Renamed column from `table`: `"X = A"`
- Calculated column: `"X = A * sqrt(B)"`

</Param>
</ParamTable>

## Returns

A new table that includes one formula column for each argument.

## Examples

The following example returns only the column `B`.

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

result = source.view(formulas=["B"])
```

In the following example, the new table contains source column `A` (renamed as `X`), and column `B`.

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

result = source.view(formulas=["X = A", "B"])
```

The following example creates a table containing column `A`, column `B` (renamed as `X`), and the square root of column `C` (renamed as `Y`). While no memory is used to store column `Y`, the square root function is evaluated every time a cell in column `Y` is accessed.

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
result = source.view(formulas=["A", "X = B", "Y = sqrt(C)"])
```

## Related documentation

- [Choose the right selection method for your query](../../../how-to-guides/use-select-view-update.md#choose-the-right-column-selection-method)
- [Create a new table](../../../how-to-guides/new-and-empty-table.md#new_table)
- [Formulas](../../../how-to-guides/formulas.md)
- [How to select, view, and update data](../../../how-to-guides/use-select-view-update.md)
- [Parallelization](../../../conceptual/query-engine/parallelization.md)
- [Selectable](../../query-language/types/Selectable.md)
- [select](./select.md)
- [update_view](./update-view.md)
- [update](./update.md)
- [lazy_update](./lazy-update.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/api/TableOperations.html#view(java.lang.String...))
- [Pydoc](/core/pydoc/code/deephaven.table.html#deephaven.table.Table.view)
