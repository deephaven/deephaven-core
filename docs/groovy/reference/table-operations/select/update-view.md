---
title: updateView
---

The `updateView` method creates a new table containing a new formula column for each argument.

A formula column is [not stored in memory](../../../how-to-guides/use-select-view-update.md#store-new-columns-in-memory-or-calculate-them-on-demand). Instead, the table stores the column's formula and recalculates each cell every time the cell is accessed.

> [!NOTE]
> `updateView` fits best when:
>
> 1. the formula is fast to compute,
> 2. only a small portion of the data is being accessed,
> 3. cells are accessed very few times, or
> 4. memory usage must be minimized.
>
> [`select`](./select.md), [`view`](./view.md), [`update`](./update.md), and [`lazyUpdate`](./lazy-update.md) differ from `updateView` in memory and computation costs.

> [!CAUTION]
> When using [`view`](./view.md) or `updateView`, non-deterministic methods (e.g., random numbers, current time, or mutable structures) produce _unstable_ results. Downstream operations on these results produce _undefined_ behavior. Non-deterministic methods belong in [`select`](./select.md) or [`update`](./update.md) instead.

> [!CAUTION]
> [`view`](./view.md) and `updateView` compute cells when a reader accesses them, on any thread and in any order, so they cannot guarantee [evaluation order](../../../conceptual/query-engine/parallelization.md#serial-execution). They throw an exception for a [`Selectable`](../../query-language/types/Selectable.md) that respects [Barriers](../../../conceptual/query-engine/parallelization.md#barriers), as the [Parameters](#parameters) section describes. A formula that must run in order belongs in [`select`](./select.md) or [`update`](./update.md) instead.

## Syntax

```
table.updateView(columns...)
table.updateView(columns)
```

## Parameters

<ParamTable>
<Param name="columns" type="String...">

Formulas to compute columns in the new table; e.g., `"X = A * sqrt(B)"`.

</Param>
<Param name="columns" type="Collection<? extends Selectable>">

One [`Selectable`](../../query-language/types/Selectable.md) for each column to compute in the new table. A [`Selectable`](../../query-language/types/Selectable.md) pairs a formula with [concurrency controls](../../query-language/types/ConcurrencyControl.md) such as [`withSerial`](../../query-language/types/Selectable.md#withserial). `updateView` throws an exception for a [`Selectable`](../../query-language/types/Selectable.md) that respects [Barriers](../../../conceptual/query-engine/parallelization.md#barriers). It also throws for a [`Selectable`](../../query-language/types/Selectable.md) that runs [serially](../../../conceptual/query-engine/parallelization.md#serial-execution) when [`QueryTable.statelessSelectByDefault`](../../../conceptual/query-table-configuration.md#stateless-by-default) is `true`, which is the default.

</Param>
</ParamTable>

## Returns

A new table that includes all the original columns from the source table and the newly defined formula columns.

## Examples

In the following example, `updateView` creates a table that contains the columns from the source table plus two new columns, `X` and `Y`. The result looks the same as the result of [`update`](./update.md). The column `X = B` refers to the data of column `B`, so it needs no calculation. `Y = sqrt(C)` is a formula column: `updateView` stores no values for it and recalculates it when a reader accesses a cell.

```groovy order=source,result
source = newTable(
    stringCol("A", "The", "At", "Is", "On"),
    intCol("B", 1, 2, 3, 4),
    intCol("C", 5, 6, 7, 8)
)

result = source.updateView("X = B", "Y = sqrt(C)")
```

The following example shows how to create a new boolean column via regex filtering.

```groovy order=iris,irisElevenChars,irisVirginica
import static io.deephaven.csv.CsvTools.readCsv
import io.deephaven.api.filter.FilterPattern.Mode
import java.util.regex.Pattern

iris = readCsv(
    "https://media.githubusercontent.com/media/deephaven/examples/main/Iris/csv/iris.csv"
)

filterElevenChars = FilterPattern.of(ColumnName.of("Class"), Pattern.compile("..........."), Mode.MATCHES, false)
filterVirginica = FilterPattern.of(ColumnName.of("Class"), Pattern.compile("virginica"), Mode.FIND, false)

irisElevenChars = iris.updateView(List.of(Selectable.of(ColumnName.of("IsElevenChars"), filterElevenChars)))
irisVirginica = iris.updateView(List.of(Selectable.of(ColumnName.of("IsVirginica"), filterVirginica)))
```

In the result, `IsElevenChars` is `true` for rows whose `Class` is exactly 11 characters long, and `IsVirginica` is `true` for rows whose `Class` contains `virginica`.

## Related documentation

- [Choose the right selection method for your query](../../../how-to-guides/use-select-view-update.md#choose-the-right-column-selection-method)
- [Create a new table](../../../how-to-guides/new-and-empty-table.md#newtable)
- [Formulas](../../../how-to-guides/formulas.md)
- [How to select, view, and update data](../../../how-to-guides/use-select-view-update.md)
- [Parallelization](../../../conceptual/query-engine/parallelization.md)
- [Selectable](../../query-language/types/Selectable.md)
- [Filter table data](../../../how-to-guides/use-filters.md)
- [select](./select.md)
- [view](./view.md)
- [update](./update.md)
- [lazyUpdate](./lazy-update.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/api/TableOperations.html#updateView(java.lang.String...))
