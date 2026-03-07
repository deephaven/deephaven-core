---
title: update
---

The `update` method creates a new table containing a new, in-memory column for each argument.

When using `update`, the new columns are evaluated and stored in memory. Existing columns are referenced without additional memory allocation.

> [!NOTE]
> The syntax for the `update`, [`updateView`](./update-view.md), and [`lazyUpdate`](./lazy-update.md) methods is identical, as is the resulting table. `update` is recommended when:
>
> 1. all the source columns are desired in the result,
> 2. the formula is expensive to evaluate,
> 3. cells are accessed many times, and/or
> 4. a large amount of memory is available.
>
> When memory usage or computation needs to be reduced, consider using `select`, `view`, `updateView`, or `lazyUpdate`. These methods have different memory and computation expenses.

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

Formulas to compute columns in the new table; e.g., `"X = A * sqrt(B)"`.

</Param>
</ParamTable>

## Returns

A new table that includes all the original columns from the source table and the newly defined in-memory columns.

## Examples

In the following example, the new columns (`A`, `X`, and `Y`) allocate memory and are immediately populated with values. Columns `B` and `C` refer to columns in the source table and do not allocate memory.

```groovy order=source,result
source = newTable(
    stringCol("A", "The", "At", "Is", "On"),
    intCol("B", 1, 2, 3, 4),
    intCol("C", 5, 6, 7, 8)
)

result = source.update("A", "X = B", "Y = sqrt(C)")
```

## Serial execution

By default, Deephaven parallelizes `update` calculations across multiple CPU cores. If your formula uses global variables or depends on row order, use `.withSerial` to force sequential processing.

```groovy order=result
import io.deephaven.api.Selectable
import java.util.concurrent.atomic.AtomicInteger

counter = new AtomicInteger(0)

col = Selectable.parse("ID = counter.getAndIncrement()").withSerial()
result = emptyTable(10).update([col])
```

For more information, see [Parallelization](../../../conceptual/query-engine/parallelization.md).

## Related documentation

- [Create a new table](../../../how-to-guides/new-and-empty-table.md#newtable)
- [Choose the right selection method for your query](../../../how-to-guides/use-select-view-update.md#choose-the-right-column-selection-method)
- [How to select, view, and update data](../../../how-to-guides/use-select-view-update.md)
- [Parallelization](../../../conceptual/query-engine/parallelization.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/api/TableOperations.html#update(java.lang.String...))
