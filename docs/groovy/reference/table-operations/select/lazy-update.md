---
title: lazyUpdate
---

The `lazyUpdate` method creates a new table containing a new, cached, formula column for each argument.

When using `lazyUpdate`, cell values are stored in memory in a cache. Column formulas are computed on-demand and defer computation until it is required. Because it caches the results for the set of input values, the same input values will never be computed twice. Existing columns are referenced without additional memory allocation.

> [!NOTE]
> The syntax for the `lazyUpdate`, [`updateView`](./update-view.md), and [`update`](./update.md) methods is identical, as is the resulting table.
>
> `lazyUpdate` is recommended for small sets of unique input values. In this case, `lazyUpdate` uses less memory than `update` and requires less computation than `updateView`. However, if there are many unique input values, `update` will be more efficient because `lazyUpdate` stores the formula inputs and result in a map, whereas `update` stores the values more compactly in an array.

## Syntax

```
table.lazyUpdate(columns...)
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

A new table that includes all the original columns from the source table and the newly defined columns.

## Examples

In the following example, a new table is created, containing the square root of column `C`. Because column `C` only contains the values 2 and 5, `sqrt(2)` is computed exactly one time, and `sqrt(5)` is computed exactly one time. The values are cached for future use.

```groovy order=source,result
source = newTable(
    stringCol("A", "The", "At", "Is", "On"),
    intCol("B", 1, 2, 3, 4),
    intCol("C", 5, 2, 5, 5)
)

result = source.lazyUpdate("Y = sqrt(C)")
```

## Related documentation

- [Create a new table](../../../how-to-guides/new-and-empty-table.md#newtable)
- [Choose the right selection method](../../../how-to-guides/use-select-view-update.md#choose-the-right-column-selection-method)
- [How to select, view, and update data](../../../how-to-guides/use-select-view-update.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/api/TableOperations.html#dropColumns(java.lang.String...))
