---
title: select
slug: ./select
---

The `select` method creates a new in-memory table that includes one column for each argument. Any columns not specified in the arguments do not appear in the resulting table.

When using `select`, the entire requested dataset is evaluated and stored in memory.

> [!NOTE]
> `select` accepts [formula strings](../../../how-to-guides/formulas.md) and [`Selectable`](../../query-language/types/Selectable.md) objects. [`view`](./view.md) accepts only [formula strings](../../../how-to-guides/formulas.md). Both methods return a table with one column for each argument. `select` stores those columns in memory, while [`view`](./view.md) stores formulas and recalculates cells on access.
>
> `select` fits best when:
>
> 1. not all the source columns are desired in the result,
> 2. the formula is expensive to evaluate,
> 3. cells are accessed many times, and/or
> 4. a large amount of memory is available.
>
> [`view`](./view.md), [`update_view`](./update-view.md), [`update`](./update.md), and [`lazy_update`](./lazy-update.md) differ from `select` in memory and computation costs.

## Syntax

```
select(formulas: Optional[Union[str, Sequence[str], Selectable, Sequence[Selectable]]] = None) -> Table
```

## Parameters

<ParamTable>
<Param name="formulas" type="Optional[Union[str, Sequence[str], Selectable, Sequence[Selectable]]]" Optional>

Formulas to compute columns in the new table:

- `NULL`: returns all columns
- Column from `table`: `"A"` (equivalent to `"A = A"`)
- Renamed column from `table`: `"X = A"`
- Calculated column: `"X = A * sqrt(B)"`
- A [`Selectable`](../../query-language/types/Selectable.md): a formula plus concurrency controls such as [`with_serial`](../../query-language/types/Selectable.md#with_serial)

</Param>
</ParamTable>

## Returns

A new in-memory table that includes one column for each argument. If no arguments are provided, the result has one column for each column of the source table.

## Examples

In the following example, `select` has zero arguments, so all columns are selected. The result holds every column in memory. Columns that are already in memory, as here, are reused, and any other column is evaluated and stored.

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

result = source.select()
```

In the following example, column `B` is selected for the new table.

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

result = source.select(formulas=["B"])
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
result = source.select(formulas=["X = A", "B"])
```

In the following example, mathematical operations are evaluated and stored in memory for the new table.

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

result = source.select(formulas=["A", "X = B", "Y = sqrt(C)"])
```

## Related documentation

- [Choose the right selection method for your query](../../../how-to-guides/use-select-view-update.md#choose-the-right-column-selection-method)
- [Create a new table](../../../how-to-guides/new-and-empty-table.md#new_table)
- [Formulas](../../../how-to-guides/formulas.md)
- [How to select, view, and update data](../../../how-to-guides/use-select-view-update.md)
- [Parallelization](../../../conceptual/query-engine/parallelization.md)
- [Selectable](../../query-language/types/Selectable.md)
- [view](./view.md)
- [update_view](./update-view.md)
- [update](./update.md)
- [lazy_update](./lazy-update.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/api/TableOperations.html#select(java.lang.String...))
- [Pydoc](/core/pydoc/code/deephaven.table.html#deephaven.table.Table.select)
