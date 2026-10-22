---
title: Group and ungroup data
---

This guide shows you how to group and ungroup table data in Deephaven.

To compare grouping with Deephaven's aggregation operations, and to choose between them, see [Aggregations in Deephaven](../conceptual/aggregations.md).

> [!TIP]
> Many Deephaven table operations, such as [`aggBy`](../reference/table-operations/group-and-aggregate/aggBy.md), group rows internally and calculate summary statistics for each group. To compute a statistic per group, pass the matching [combined aggregation](./combined-aggregations.md) to `aggBy`, such as [`AggSum`](../reference/table-operations/group-and-aggregate/AggSum.md) for a sum or [`AggAvg`](../reference/table-operations/group-and-aggregate/AggAvg.md) for an average. You can also use a [dedicated aggregation](./dedicated-aggregations.md) such as [`sumBy`](../reference/table-operations/group-and-aggregate/sumBy.md). Grouping the data first with [`groupBy`](../reference/table-operations/group-and-aggregate/groupBy.md) or [`AggGroup`](../reference/table-operations/group-and-aggregate/AggGroup.md) and then calculating over the grouped [vectors](../conceptual/arrays-vs-vectors.md) is usually slower.

This guide uses a table of apple data called `apples` created using [`newTable`](../reference/table-operations/create/newTable.md). Many of the grouping and ungrouping examples use this table. If you are unfamiliar with creating tables from scratch using `newTable`, see the guide on [creating new tables](./new-and-empty-table.md#newtable).

Use the code below to create the `apples` table:

```groovy test-set=1
apples = newTable(
    stringCol("Type", "Granny Smith", "Granny Smith", "Gala", "Gala", "Golden Delicious", "Golden Delicious"),
    stringCol("Color", "Green", "Green", "Red-Green", "Orange-Green", "Yellow", "Yellow"),
    intCol("WeightGrams", 102, 85, 79, 92, 78, 99),
    intCol("Calories", 53, 48, 51, 61, 46, 57)
)
```

## `groupBy`

The [`groupBy`](../reference/table-operations/group-and-aggregate/groupBy.md) method groups rows by the values in one or more grouping columns, called keys. Rows that share the same key values form one group. For each group, `groupBy` collects every non-key column into a Deephaven [vector](../conceptual/arrays-vs-vectors.md), and the values in each vector keep their order from the input table.

If you call [`groupBy`](../reference/table-operations/group-and-aggregate/groupBy.md) with no grouping columns, it puts all rows in one group. The result has a single row, and each column holds one [vector](../conceptual/arrays-vs-vectors.md) of all its values:

```groovy test-set=1
applesByNoColumn = apples.groupBy()
```

If you pass one grouping column, [`groupBy`](../reference/table-operations/group-and-aggregate/groupBy.md) groups rows into [vectors](../conceptual/arrays-vs-vectors.md) by each unique value in that column:

```groovy test-set=1
applesByType = apples.groupBy("Type")
```

If you pass more than one grouping column, [`groupBy`](../reference/table-operations/group-and-aggregate/groupBy.md) groups rows into [vectors](../conceptual/arrays-vs-vectors.md) by each unique combination of values in those columns:

```groovy test-set=1
applesByTypeAndColor = apples.groupBy("Type", "Color")
```

To group data by conditions on columns, first add columns that hold the conditions, then group by those new columns. The example below uses [`updateView`](../reference/table-operations/select/update-view.md) to add the `Class` and `Diet` columns. To choose between `updateView` and the other column selection methods, see [Choose the right column selection method](./use-select-view-update.md#choose-the-right-column-selection-method).

```groovy test-set=1
applesByClassAndDiet = apples.updateView(
    "Class = (WeightGrams < 90) ? `Light` : `Heavy`",
    "Diet = (Calories < 50) ? `Allowed` : `Not Allowed`")
    .groupBy("Class", "Diet")
```

## `AggGroup`

The [`AggGroup`](../reference/table-operations/group-and-aggregate/AggGroup.md) aggregation collects the values of each listed column into a [vector](../conceptual/arrays-vs-vectors.md), one vector per group. You pass it to [`aggBy`](../reference/table-operations/group-and-aggregate/aggBy.md), which forms the groups from its key columns. `AggGroup` is one of the [combined aggregations](./combined-aggregations.md) that `aggBy` accepts.

Use [`AggGroup`](../reference/table-operations/group-and-aggregate/AggGroup.md) instead of [`groupBy`](../reference/table-operations/group-and-aggregate/groupBy.md) when you want the grouped vectors in the same [`aggBy`](../reference/table-operations/group-and-aggregate/aggBy.md) call as other aggregations, such as a sum or an average of another column. Unlike `groupBy`, which collects every non-key column, `AggGroup` collects only the columns you list, and you must list at least one.

In this example, we group the `Color` values by `Type` and calculate the average weight and calories of each type in the same call:

```groovy test-set=1 order=applesAggByType
applesAggByType = apples.aggBy(
    [AggGroup("Color"), AggAvg("AvgWeightGrams = WeightGrams", "AvgCalories = Calories")],
    "Type"
)
```

If you don't pass any grouping columns to [`aggBy`](../reference/table-operations/group-and-aggregate/aggBy.md), all rows form a single group, and [`AggGroup`](../reference/table-operations/group-and-aggregate/AggGroup.md) groups every value of each column you list:

```groovy test-set=1 order=applesAggNoKey
applesAggNoKey = apples.aggBy(AggGroup("Type", "Color", "WeightGrams", "Calories"))
```

## `ungroup`

The [`ungroup`](../reference/table-operations/group-and-aggregate/ungroup.md) method is the opposite of [`groupBy`](../reference/table-operations/group-and-aggregate/groupBy.md). It expands each [array](../reference/query-language/types/arrays.md) or [vector](../conceptual/arrays-vs-vectors.md) cell into one row per element. With no arguments, `ungroup` expands every array or vector column. When you pass one or more column names, it expands only those columns.

The example below calls [`ungroup`](../reference/table-operations/group-and-aggregate/ungroup.md) with no arguments on `applesByClassAndDiet`, the table from the [`groupBy`](../reference/table-operations/group-and-aggregate/groupBy.md) example above. In the result:

- Each row holds one apple, with a single value in each cell.
- Rows are ordered by group rather than in their original order.
- The `Class` and `Diet` columns remain.

```groovy test-set=1
newApples = applesByClassAndDiet.ungroup()
```

The example below uses [`ungroup`](../reference/table-operations/group-and-aggregate/ungroup.md) to expand only the `Color` column in `applesByClassAndDiet`. The `Type`, `WeightGrams`, and `Calories` columns still contain [vectors](../conceptual/arrays-vs-vectors.md):

```groovy test-set=1
applesUngroupedByColor = applesByClassAndDiet.ungroup("Color")
```

### Ungroup vectors and Java arrays

The columns you ungroup don't have to come from [`groupBy`](../reference/table-operations/group-and-aggregate/groupBy.md). [`ungroup`](../reference/table-operations/group-and-aggregate/ungroup.md) also expands Java arrays that you create in a formula, and it can expand a [vector](../conceptual/arrays-vs-vectors.md) column and an array column in the same call.

The example below uses the [`emptyTable`](../reference/table-operations/create/emptyTable.md) method to create a table with two columns and one row. Each column contains a Java array with three elements. The [`ungroup`](../reference/table-operations/group-and-aggregate/ungroup.md) method expands both columns into three rows.

```groovy order=t,tUngrouped
t = emptyTable(1).update("X = new int[]{1, 2, 3}", "Z = new int[]{4, 5, 6}")
tUngrouped = t.ungroup()
```

The example below creates column `X` as a vector with [`groupBy`](../reference/table-operations/group-and-aggregate/groupBy.md) and column `Z` as a Java array. It then uses [`ungroup`](../reference/table-operations/group-and-aggregate/ungroup.md) to expand both columns at the same time.

```groovy order=t,tUngrouped
t = newTable(
    intCol("X", 1, 2, 3)
).groupBy().update("Z = new int[]{4, 5, 6}")

tUngrouped = t.ungroup()
```

### Ungroup arrays of different lengths

By default, the [`ungroup`](../reference/table-operations/group-and-aggregate/ungroup.md) method cannot expand a row that contains [arrays](../reference/query-language/types/arrays.md) of different lengths.

The example below creates a table with two columns and one row.

```groovy test-set=2 order=t
t = emptyTable(1).update("X = new int[]{1, 2, 3}", "Z = new int[]{4, 5}")
```

Each column in the above table contains a Java array, but one has three elements and the other has two. Since the arrays are not the same size, calling [`ungroup`](../reference/table-operations/group-and-aggregate/ungroup.md) with no column names results in an error.

```groovy test-set=2 should-fail
tUngrouped = t.ungroup()  // This results in an error
```

The error message includes `Array sizes differ at row key 0 (position 0), X has size 3, Z has size 2`.

To ungroup [arrays](../reference/query-language/types/arrays.md) of different lengths, ungroup each column separately:

```groovy order=t,tUngroupedByX,tUngroupedByZ
t = emptyTable(1).update("X = new int[]{1, 2, 3}", "Z = new int[]{4, 5}")
tUngroupedByX = t.ungroup("X")
tUngroupedByZ = t.ungroup("Z")
```

Alternatively, call [`ungroup(true)`](../reference/table-operations/group-and-aggregate/ungroup.md) to pad the shorter arrays with null values. The `true` value sets the `nullFill` parameter:

```groovy order=t,tUngroupedNullFill
t = emptyTable(1).update("X = new int[]{1, 2, 3}", "Z = new int[]{4, 5}")
tUngroupedNullFill = t.ungroup(true)
```

## Null values

[`groupBy`](../reference/table-operations/group-and-aggregate/groupBy.md) keeps null values. Each null becomes an element of the group's [vector](../conceptual/arrays-vs-vectors.md), and the UI displays it as empty. When [`ungroup`](../reference/table-operations/group-and-aggregate/ungroup.md) expands the vector, each null element becomes a null cell in the corresponding column.

The example below uses the [`emptyTable`](../reference/table-operations/create/emptyTable.md) method and the [ternary operator](./ternary-if-how-to.md) to create a table with two columns and five rows. The first and second rows of column `Z` contain null values. [`groupBy`](../reference/table-operations/group-and-aggregate/groupBy.md) keeps the nulls in the `Z` vector, and [`ungroup`](../reference/table-operations/group-and-aggregate/ungroup.md) turns them back into null cells.

```groovy order=t,tBy,newT
t = emptyTable(5).update("X = i", "Z = i < 2 ? NULL_INT : i - 2")
tBy = t.groupBy()
newT = tBy.ungroup()
```

A cell that holds a null array, rather than an array of nulls, produces no rows when you ungroup it. The example below uses the [`emptyTable`](../reference/table-operations/create/emptyTable.md) method to create a table with one column and one row, where the single cell holds a null Java array. Calling [`ungroup`](../reference/table-operations/group-and-aggregate/ungroup.md) on this table results in an empty table with one column.

```groovy order=t,tUngrouped
t = emptyTable(1).update("X = (int[])(null)")
tUngrouped = t.ungroup()
```

## Related documentation

- [Aggregations in Deephaven](../conceptual/aggregations.md)
- [Create a new table](./new-and-empty-table.md#newtable)
- [Choose the right column selection method](./use-select-view-update.md#choose-the-right-column-selection-method)
- [Formulas in query strings](./formulas.md)
- [Filters in query strings](./filters.md)
- [Operators in query strings](./operators.md)
- [Arrays](../reference/query-language/types/arrays.md)
- [Arrays vs vectors](../conceptual/arrays-vs-vectors.md)
- [Combined aggregations](./combined-aggregations.md)
- [Dedicated aggregations](./dedicated-aggregations.md)
- [`aggBy`](../reference/table-operations/group-and-aggregate/aggBy.md)
- [`AggGroup`](../reference/table-operations/group-and-aggregate/AggGroup.md)
- [`emptyTable`](../reference/table-operations/create/emptyTable.md)
- [`groupBy`](../reference/table-operations/group-and-aggregate/groupBy.md)
- [`newTable`](../reference/table-operations/create/newTable.md)
- [`ungroup`](../reference/table-operations/group-and-aggregate/ungroup.md)
