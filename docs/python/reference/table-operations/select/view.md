---
title: view
---

The `view` method creates a new formula table that includes one column for each argument.

When using `view`, the data being requested is not stored in memory. Rather, a formula is stored that is used to recalculate each cell every time it is accessed.

> [!NOTE]
> The syntax for the `view` and [`select`](./select.md) methods is identical, as is the resulting table. `view` is recommended when:
>
> 1. the formula is fast to compute,
> 2. only a small portion of the data is being accessed,
> 3. cells are accessed very few times, or
> 4. memory usage must be minimized.
>
> When memory usage or computation needs to be reduced, consider using `select`, `update_view`, `update`, or `lazy_update`. These methods have different memory and computation expenses.

> [!CAUTION]
> When using `view` or [`update_view`](./update-view.md), non-deterministic methods (e.g., random numbers, current time, or mutable structures) produce _unstable_ results. Downstream operations on these results produce _undefined_ behavior. Non-deterministic methods should use [`select`](./select.md) or [`update`](./update.md) instead.

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

A new formula table that includes one column for each argument.

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

- [Create a new table](../../../how-to-guides/new-and-empty-table.md#new_table)
- [How to select, view, and update data](../../../how-to-guides/use-select-view-update.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/api/TableOperations.html#view(java.lang.String...))
- [Pydoc](/core/pydoc/code/deephaven.table.html#deephaven.table.Table.view)
