---
title: updateView
---

The `updateView` method creates a new table containing a new, formula column for each argument.

When using `updateView`, the new columns are not stored in memory. Rather, a formula is stored that is used to recalculate each cell every time it is accessed.

> [!NOTE]
> The syntax for the `updateView`, [`update`](./update.md), and [`lazyUpdate`](./lazy-update.md) methods is identical, as is the resulting table. `updateView` is recommended when:
>
> 1. the formula is fast to compute,
> 2. only a small portion of the data is being accessed,
> 3. cells are accessed very few times, or
> 4. memory usage must be minimized.
>
> For other cases, consider using `select`, `view`, `update`, or `lazyUpdate`. These methods have different memory and computation expenses.

> [!CAUTION]
> When using [`view`](./view.md) or `updateView`, non-deterministic methods (e.g., random numbers, current time, or mutable structures) produce _unstable_ results. Downstream operations on these results produce _undefined_ behavior. Non-deterministic methods should use [`select`](./select.md) or [`update`](./update.md) instead.

## Syntax

```
table.updateView(columns...)
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

A new table that includes all the original columns from the source table and the newly defined formula columns.

## Examples

In the following example, a new table containing the columns from the source table plus two new columns, `X` and `Y`, is created.

```groovy order=source,result
source = newTable(
    stringCol("A", "The", "At", "Is", "On"),
    intCol("B", 1, 2, 3, 4),
    intCol("C", 5, 6, 7, 8)
)

result = source.updateView("X = B", "Y = sqrt(C)")
```

The following example shows how to create a new boolean column via regex filtering.

```groovy order=iris,irisWhereElevenChars,irisWhereVirginica
import static io.deephaven.csv.CsvTools.readCsv
import io.deephaven.api.filter.FilterPattern.Mode
import java.util.regex.Pattern

iris = readCsv(
    "https://media.githubusercontent.com/media/deephaven/examples/main/Iris/csv/iris.csv"
)

filterElevenChars = FilterPattern.of(ColumnName.of("Class"), Pattern.compile("..........."), Mode.MATCHES, false)
filterRegexMatch = FilterPattern.of(ColumnName.of("Class"), Pattern.compile("virginica"), Mode.FIND, false)

irisWhereElevenChars = iris.updateView(List.of(Selectable.of(ColumnName.of("IsElevenChars"), filterElevenChars)))
irisWhereVirginica = iris.updateView(List.of(Selectable.of(ColumnName.of("IsVirginica"), filterRegexMatch)))
```

## Related documentation

- [Create a new table](../../../how-to-guides/new-and-empty-table.md#newtable)
- [Choose the right selection method for your query](../../../how-to-guides/use-select-view-update.md#choose-the-right-column-selection-method)
- [How to select, view, and update data](../../../how-to-guides/use-select-view-update.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/api/TableOperations.html#updateView(java.lang.String...))
