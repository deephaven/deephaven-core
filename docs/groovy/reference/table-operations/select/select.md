---
title: select
slug: ./select
---

The `select` method creates a new in-memory table that includes one column for each argument. Any columns not specified in the arguments will not appear in the resulting table.

When using `select`, the entire requested dataset is evaluated and stored in memory.

> [!NOTE]
> The syntax for the `select` and [`view`](./view.md) methods is identical, as is the resulting table. `select` is recommended when:
>
> 1. not all the source columns are desired in the result,
> 2. the formula is expensive to evaluate,
> 3. cells are accessed many times, and/or
> 4. a large amount of memory is available.
>
> When memory usage or computation needs to be reduced, consider using `view`, `updateView`, `update`, or `lazyUpdate`. These methods have different memory and computation expenses.

## Syntax

```
table.select()
table.select(columns...)
```

## Parameters

<ParamTable>
<Param name="columns" type="String...">

Formulas to compute columns in the new table:

- `NULL`: returns all columns
- Column from `table`: `"A"` (equivalent to `"A = A"`)
- Renamed column from `table`: `"X = A"`
- Calculated column: `"X = A * sqrt(B)"`

</Param>
<Param name="columns" type="Collection<? extends Selectable>">

Formulas to compute columns in the new table:

- `NULL`: returns all columns
- Column from `table`: `"A"` (equivalent to `"A = A"`)
- Renamed column from `table`: `"X = A"`
- Calculated column: `"X = A * sqrt(B)"`

</Param>
</ParamTable>

## Returns

A new in-memory table that includes one column for each argument. If no arguments are provided, there will be one column for each column of the source table.

## Examples

In the following example, `select` has zero arguments. All columns are selected. While this selection appears to do nothing, it is creating a compact, in-memory representation of the input table, with all formulas evaluated.

```groovy order=source,result
source = newTable(
    stringCol("A", "The", "At", "Is", "On"),
    intCol("B", 1, 2, 3, 4),
    intCol("C", 5, 6, 7, 8)
)

result = source.select()
```

In the following example, column `B` is selected for the new table.

```groovy order=source,result
source = newTable(
    stringCol("A", "The", "At", "Is", "On"),
    intCol("B", 1, 2, 3, 4),
    intCol("C", 5, 6, 7, 8)
)

result = source.select("B")
```

In the following example, the new table contains source column `A` (renamed as `X`), and column `B`.

```groovy order=source,result
source = newTable(
    stringCol("A", "The", "At", "Is", "On"),
    intCol("B", 1, 2, 3, 4),
    intCol("C", 5, 6, 7, 8)
)

result = source.select("X = A", "B")
```

In the following example, mathematical operations are evaluated and stored in memory for the new table.

```groovy order=source,result
source = newTable(
    stringCol("A", "The", "At", "Is", "On"),
    intCol("B", 1, 2, 3, 4),
    intCol("C", 5, 6, 7, 8)
)

result = source.select("A", "X = B", "Y = sqrt(C)")
```

## Related documentation

- [Create a new table](../../../how-to-guides/new-and-empty-table.md#newtable)
- [Choose the right selection method](../../../how-to-guides/use-select-view-update.md#choose-the-right-column-selection-method)
- [How to select, view, and update data](../../../how-to-guides/use-select-view-update.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/api/TableOperations.html#select(java.lang.String...))
