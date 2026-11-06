---
title: update
---

The `update` method creates a new table containing a new, [in-memory](../../../how-to-guides/use-select-view-update.md#store-new-columns-in-memory-or-calculate-them-on-demand) column for each argument.

When using `update`, the new columns are evaluated and [stored in memory](../../../how-to-guides/use-select-view-update.md#store-new-columns-in-memory-or-calculate-them-on-demand). Existing columns are referenced without additional memory allocation.

> [!NOTE]
> `update` accepts [formula strings](../../../how-to-guides/formulas.md) and [`Selectable`](../../query-language/types/Selectable.md) objects. [`update_view`](./update-view.md) and [`lazy_update`](./lazy-update.md) accept only formula strings.
>
> `update` fits best when:
>
> 1. all the source columns are desired in the result,
> 2. the formula is expensive to evaluate,
> 3. cells are accessed many times, and/or
> 4. a large amount of memory is available.
>
> [`select`](./select.md), [`view`](./view.md), [`update_view`](./update-view.md), and [`lazy_update`](./lazy-update.md) differ from `update` in memory and computation costs.

## Syntax

```
update(formulas: Union[str, Sequence[str], Selectable, Sequence[Selectable]]) -> Table
```

## Parameters

<ParamTable>
<Param name="formulas" type="Union[str, Sequence[str], Selectable, Sequence[Selectable]]">

Formulas to compute columns in the new table; e.g., `"X = A * sqrt(B)"`. A [`Selectable`](../../query-language/types/Selectable.md) pairs a formula with [concurrency controls](../../query-language/types/ConcurrencyControl.md) such as [`with_serial`](../../query-language/types/Selectable.md#with_serial).

</Param>
</ParamTable>

## Returns

A new table that includes all the original columns from the source table and the newly defined in-memory columns.

## Examples

In the following example, `update` adds two columns. The new column `Y` allocates memory and is populated with values immediately. The new column `X = B` points to the same in-memory data as `B`, so it allocates none. The source columns `A`, `B`, and `C` are already in memory, because [`new_table`](../create/newTable.md) creates them there, so referencing them allocates no memory either.

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

## Run an update serially

A formula that has side effects or depends on row order can use [`with_serial`](../../query-language/types/Selectable.md#with_serial) to force [serial execution](../../../conceptual/query-engine/parallelization.md#serial-execution) in row set order. Without it, `update` can split a large input across CPU cores. With default settings, that means 4,194,304 rows or more, as [`QueryTable.minimumParallelSelectRows`](../../../conceptual/query-table-configuration.md#parallel-processing-with-select) sets. A formula backed by a Python callback is split only on a [free-threaded](../../../conceptual/query-engine/parallelization.md#python-formulas-and-free-threading) Python build.

The following example marks the `ID` column as serial with [`with_serial`](../../query-language/types/Selectable.md#with_serial). Each row calls `get_next_id` in row set order, so the counter increments one row at a time and `ID` holds the values 1 through 10 in row order. The example evaluates 10 rows, far below the parallelization threshold, so it shows the ordering guarantee rather than a speedup.

```python order=result
from deephaven.table import Selectable
from deephaven import empty_table

counter = 0


def get_next_id() -> int:
    global counter
    counter += 1
    return counter


col = Selectable.parse("ID = get_next_id()").with_serial()
result = empty_table(10).update(col)
```

For more information, see [Parallelization](../../../conceptual/query-engine/parallelization.md).

## Related documentation

- [Choose the right selection method for your query](../../../how-to-guides/use-select-view-update.md#choose-the-right-column-selection-method)
- [Create a new table](../../../how-to-guides/new-and-empty-table.md#new_table)
- [Formulas](../../../how-to-guides/formulas.md)
- [How to select, view, and update data](../../../how-to-guides/use-select-view-update.md)
- [Parallelization](../../../conceptual/query-engine/parallelization.md)
- [Selectable](../../query-language/types/Selectable.md)
- [with_serial](../../query-language/types/Selectable.md#with_serial)
- [select](./select.md)
- [view](./view.md)
- [update_view](./update-view.md)
- [lazy_update](./lazy-update.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/api/TableOperations.html#update(java.lang.String...))
- [Pydoc](/core/pydoc/code/deephaven.table.html#deephaven.table.Table.update)
