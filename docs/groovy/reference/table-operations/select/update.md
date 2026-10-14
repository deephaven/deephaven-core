---
title: update
---

The `update` method creates a new table containing a new, in-memory column for each argument.

When using `update`, the new columns are evaluated and stored in memory. Existing columns are referenced without additional memory allocation.

> [!NOTE]
> `update`, [`updateView`](./update-view.md), and [`lazyUpdate`](./lazy-update.md) take the same arguments: [formula strings](../../../how-to-guides/formulas.md) or a collection of [`Selectable`](../../query-language/types/Selectable.md) objects. All three methods return a table with all the source columns plus the new columns. `update` stores the new columns in memory, while [`updateView`](./update-view.md) stores formulas and recalculates cells on access.
>
> `update` fits best when:
>
> 1. all the source columns are desired in the result,
> 2. the formula is expensive to evaluate,
> 3. cells are accessed many times, and/or
> 4. a large amount of memory is available.
>
> [`select`](./select.md), [`view`](./view.md), [`updateView`](./update-view.md), and [`lazyUpdate`](./lazy-update.md) differ from `update` in memory and computation costs.

## Syntax

```
table.update(columns...)
```

## Parameters

<ParamTable>
<Param name="columns" type="String...">

Formulas to compute columns in the new table; e.g., `"X = A * sqrt(B)"`.

</Param>
<Param name="columns" type="Collection<? extends Selectable>">

One [`Selectable`](../../query-language/types/Selectable.md) for each column to compute in the new table. A [`Selectable`](../../query-language/types/Selectable.md) pairs a formula with concurrency controls such as [`withSerial`](../../query-language/types/Selectable.md#withserial).

</Param>
</ParamTable>

## Returns

A new table that includes all the original columns from the source table and the newly defined in-memory columns.

## Examples

In the following example, `update` adds two columns. The new column `Y` allocates memory and is immediately populated with values. The new column `X = B` points to the same in-memory data as `B`, so it allocates none. `A`, `B`, and `C` are the source table's own columns, which `newTable` holds in memory, so referencing them allocates none either.

```groovy order=source,result
source = newTable(
    stringCol("A", "The", "At", "Is", "On"),
    intCol("B", 1, 2, 3, 4),
    intCol("C", 5, 6, 7, 8)
)

result = source.update("A", "X = B", "Y = sqrt(C)")
```

## Serial execution

By default, Deephaven parallelizes `update` calculations across multiple CPU cores when the input is large enough. A formula that has side effects or depends on row order can use [`withSerial`](../../query-language/types/Selectable.md#withserial) to force [serial execution](../../../conceptual/query-engine/parallelization.md#serial-execution) in row-set order.

The following example marks the `ID` column as serial with [`withSerial`](../../query-language/types/Selectable.md#withserial). Each row calls `counter.getAndIncrement()` in row-set order, so the counter increments one row at a time and `ID` holds the values 0 through 9 in row order.

```groovy order=result
import io.deephaven.api.Selectable
import java.util.concurrent.atomic.AtomicInteger

counter = new AtomicInteger(0)

col = Selectable.parse("ID = counter.getAndIncrement()").withSerial()
result = emptyTable(10).update([col])
```

For more information, see [Parallelization](../../../conceptual/query-engine/parallelization.md).

## Related documentation

- [Choose the right selection method for your query](../../../how-to-guides/use-select-view-update.md#choose-the-right-column-selection-method)
- [Create a new table](../../../how-to-guides/new-and-empty-table.md#newtable)
- [Formulas](../../../how-to-guides/formulas.md)
- [How to select, view, and update data](../../../how-to-guides/use-select-view-update.md)
- [Parallelization](../../../conceptual/query-engine/parallelization.md)
- [Selectable](../../query-language/types/Selectable.md)
- [withSerial](../../query-language/types/Selectable.md#withserial)
- [select](./select.md)
- [view](./view.md)
- [updateView](./update-view.md)
- [lazyUpdate](./lazy-update.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/api/TableOperations.html#update(java.lang.String...))
