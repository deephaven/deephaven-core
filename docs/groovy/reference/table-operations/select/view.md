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
> When memory usage or computation needs to be reduced, consider using `select`, `updateView`, `update`, or `lazyUpdate`. These methods have different memory and computation expenses.

> [!CAUTION]
> When using `view` or [`updateView`](./update-view.md), non-deterministic methods (e.g., random numbers, current time, or mutable structures) produce _unstable_ results. Downstream operations on these results produce _undefined_ behavior. Non-deterministic methods should use [`select`](./select.md) or [`update`](./update.md) instead.
> Concurrency control (serial marking and barriers) cannot be applied to `view` or [`updateView`](./update-view.md). These operations compute results on demand and cannot enforce ordering constraints. If you need serial evaluation or barriers, use [`select`](./select.md) or [`update`](./update.md) instead. See [Parallelizing queries](../../../conceptual/query-engine/parallelization.md#serialization) for more information.

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

- [Create a new table](../../../how-to-guides/new-and-empty-table.md#newtable)
- [Choose the right selection method for your query](../../../how-to-guides/use-select-view-update.md#choose-the-right-column-selection-method)
- [How to select, view, and update data](../../../how-to-guides/use-select-view-update.md)
- [Javadoc](<https://deephaven.io/core/javadoc/io/deephaven/api/TableOperations.html#view(java.lang.String...)>)
