---
title: view
---

The [`view`](./view.md) method creates a new table that includes one formula column for each argument.

A formula column is not stored in memory. Instead, the table stores the column's formula and recalculates each cell every time the cell is accessed.

> [!NOTE]
> [`view`](./view.md) and [`select`](./select.md) take the same arguments: [formula strings](../../../how-to-guides/formulas.md) or a collection of [`Selectable`](../../query-language/types/Selectable.md) objects. [`view`](./view.md) throws an exception for a [`Selectable`](../../query-language/types/Selectable.md) that respects [Barriers](../../../conceptual/query-engine/parallelization.md#barriers), and, with the default settings, for one that runs serially. Both methods return a table with one column for each argument. [`view`](./view.md) stores formulas and recalculates cells on access, while [`select`](./select.md) stores those columns in memory.
>
> [`view`](./view.md) fits best when:
>
> 1. the formula is fast to compute,
> 2. only a small portion of the data is being accessed,
> 3. cells are accessed very few times, or
> 4. memory usage must be minimized.
>
> [`select`](./select.md), [`updateView`](./update-view.md), [`update`](./update.md), and [`lazyUpdate`](./lazy-update.md) differ from [`view`](./view.md) in memory and computation costs.

> [!CAUTION]
> When using [`view`](./view.md) or [`updateView`](./update-view.md), non-deterministic methods (e.g., random numbers, current time, or mutable structures) produce _unstable_ results. Downstream operations on these results produce _undefined_ behavior. Non-deterministic methods belong in [`select`](./select.md) or [`update`](./update.md) instead.

> [!CAUTION]
> [`view`](./view.md) and [`updateView`](./update-view.md) do not support [serial execution](../../../conceptual/query-engine/parallelization.md#serial-execution) or respecting [Barriers](../../../conceptual/query-engine/parallelization.md#barriers). These operations compute results lazily, when cells are accessed, so rows can be evaluated in any order, at any time, and by any thread. This makes ordering guarantees meaningless. A formula that requires serial evaluation or must wait for another operation belongs in [`select`](./select.md) or [`update`](./update.md) instead.

## Syntax

```
table.view(columns...)
```

## Parameters

<ParamTable>
<Param name="columns" type="String...">

Formulas to compute columns in the new table:

- Column from `table`: `"A"` (equivalent to `"A = A"`)
- Renamed column from `table`: `"X = A"`
- Calculated column: `"X = A * sqrt(B)"`

</Param>
<Param name="columns" type="Collection<? extends Selectable>">

One [`Selectable`](../../query-language/types/Selectable.md) for each column to compute in the new table. A [`Selectable`](../../query-language/types/Selectable.md) pairs a formula with concurrency controls such as [`withSerial`](../../query-language/types/Selectable.md#withserial). [`view`](./view.md) throws an exception for a [`Selectable`](../../query-language/types/Selectable.md) that respects [Barriers](../../../conceptual/query-engine/parallelization.md#barriers), and, with the default settings, for one that runs serially.

</Param>
</ParamTable>

## Returns

A new table that includes one formula column for each argument.

## Examples

The following example returns only the column `B`.

```groovy order=source,result
source = newTable(
    stringCol("A", "The", "At", "Is", "On"),
    intCol("B", 1, 2, 3, 4),
    intCol("C", 5, 6, 7, 8)
)

result = source.view("B")
```

In the following example, the new table contains source column `A` (renamed as `X`), and column `B`.

```groovy order=source,result
source = newTable(
    stringCol("A", "The", "At", "Is", "On"),
    intCol("B", 1, 2, 3, 4),
    intCol("C", 5, 6, 7, 8)
)

result = source.view("X = A", "B")
```

The following example creates a table containing column `A`, column `B` (renamed as `X`), and the square root of column `C` (renamed as `Y`). While no memory is used to store column `Y`, the square root function is evaluated every time a cell in column `Y` is accessed.

```groovy order=source,result
source = newTable(
    stringCol("A", "The", "At", "Is", "On"),
    intCol("B", 1, 2, 3, 4),
    intCol("C", 5, 6, 7, 8)
)


result = source.view("A", "X = B", "Y = sqrt(C)")
```

## Related documentation

- [Choose the right selection method for your query](../../../how-to-guides/use-select-view-update.md#choose-the-right-column-selection-method)
- [Create a new table](../../../how-to-guides/new-and-empty-table.md#newtable)
- [Formulas](../../../how-to-guides/formulas.md)
- [How to select, view, and update data](../../../how-to-guides/use-select-view-update.md)
- [Parallelization](../../../conceptual/query-engine/parallelization.md)
- [Selectable](../../query-language/types/Selectable.md)
- [select](./select.md)
- [updateView](./update-view.md)
- [update](./update.md)
- [lazyUpdate](./lazy-update.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/api/TableOperations.html#view(java.lang.String...))
