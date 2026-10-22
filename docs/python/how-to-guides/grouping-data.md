---
title: Group and ungroup data
---

This guide shows you how to group and ungroup table data in Deephaven.

To compare grouping with Deephaven's aggregation operations, and to choose between them, see [Aggregations in Deephaven](../conceptual/aggregations.md).

> [!TIP]
> Many Deephaven table operations, such as [`agg_by`](../reference/table-operations/group-and-aggregate/aggBy.md), group rows internally and calculate summary statistics for each group. To compute a statistic per group, pass the matching [combined aggregation](./combined-aggregations.md) to `agg_by`, such as [`agg.sum_`](../reference/table-operations/group-and-aggregate/AggSum.md) for a sum or [`agg.avg`](../reference/table-operations/group-and-aggregate/AggAvg.md) for an average. You can also use a [dedicated aggregation](./dedicated-aggregations.md) such as [`sum_by`](../reference/table-operations/group-and-aggregate/sumBy.md). Grouping the data first with [`group_by`](../reference/table-operations/group-and-aggregate/groupBy.md) or [`agg.group`](../reference/table-operations/group-and-aggregate/AggGroup.md) and then calculating over the grouped [vectors](../conceptual/arrays-vs-vectors.md) is usually slower.

This guide uses a table of apple data called `apples` created using [`new_table`](../reference/table-operations/create/newTable.md). Many of the grouping and ungrouping examples use this table. If you are unfamiliar with creating tables from scratch using `new_table`, see the guide on [creating new tables](./new-and-empty-table.md#new_table).

Use the code below to create the `apples` table:

```python test-set=1
from deephaven import new_table
from deephaven.column import string_col, int_col

apples = new_table(
    [
        string_col(
            "Type",
            [
                "Granny Smith",
                "Granny Smith",
                "Gala",
                "Gala",
                "Golden Delicious",
                "Golden Delicious",
            ],
        ),
        string_col(
            "Color", ["Green", "Green", "Red-Green", "Orange-Green", "Yellow", "Yellow"]
        ),
        int_col("WeightGrams", [102, 85, 79, 92, 78, 99]),
        int_col("Calories", [53, 48, 51, 61, 46, 57]),
    ]
)
```

## `group_by`

The [`group_by`](../reference/table-operations/group-and-aggregate/groupBy.md) method groups rows by the values in one or more grouping columns, called keys. Rows that share the same key values form one group. For each group, `group_by` collects every non-key column into a Deephaven [vector](../conceptual/arrays-vs-vectors.md), and the values in each vector keep their order from the input table.

If you call [`group_by`](../reference/table-operations/group-and-aggregate/groupBy.md) with no grouping columns, it puts all rows in one group. The result has a single row, and each column holds one [vector](../conceptual/arrays-vs-vectors.md) of all its values:

```python test-set=1
apples_by_no_column = apples.group_by()
```

If you pass one grouping column, [`group_by`](../reference/table-operations/group-and-aggregate/groupBy.md) groups rows into [vectors](../conceptual/arrays-vs-vectors.md) by each unique value in that column:

```python test-set=1
apples_by_type = apples.group_by(by=["Type"])
```

If you pass more than one grouping column, [`group_by`](../reference/table-operations/group-and-aggregate/groupBy.md) groups rows into [vectors](../conceptual/arrays-vs-vectors.md) by each unique combination of values in those columns:

```python test-set=1
apples_by_type_and_color = apples.group_by(by=["Type", "Color"])
```

To group data by conditions on columns, first add columns that hold the conditions, then group by those new columns. The example below uses [`update_view`](../reference/table-operations/select/update-view.md) to add the `Class` and `Diet` columns. To choose between `update_view` and the other column selection methods, see [Choose the right column selection method](./use-select-view-update.md#choose-the-right-column-selection-method).

```python test-set=1
apples_by_class_and_diet = apples.update_view(
    formulas=[
        "Class = (WeightGrams < 90) ? `Light` : `Heavy`",
        "Diet = (Calories < 50) ? `Allowed` : `Not Allowed`",
    ]
).group_by(by=["Class", "Diet"])
```

## `agg.group`

The [`agg.group`](../reference/table-operations/group-and-aggregate/AggGroup.md) aggregation collects the values of each listed column into a [vector](../conceptual/arrays-vs-vectors.md), one vector per group. You pass it to [`agg_by`](../reference/table-operations/group-and-aggregate/aggBy.md), which forms the groups from its key columns. `agg.group` is one of the [combined aggregations](./combined-aggregations.md) that `agg_by` accepts.

Use [`agg.group`](../reference/table-operations/group-and-aggregate/AggGroup.md) instead of [`group_by`](../reference/table-operations/group-and-aggregate/groupBy.md) when you want the grouped vectors in the same [`agg_by`](../reference/table-operations/group-and-aggregate/aggBy.md) call as other aggregations, such as a sum or an average of another column. Unlike `group_by`, which collects every non-key column, `agg.group` collects only the columns you list, and you must list at least one.

In this example, we group the `Color` values by `Type` and calculate the average weight and calories of each type in the same call:

```python test-set=1 order=apples_agg_by_type
from deephaven import agg

apples_agg_by_type = apples.agg_by(
    aggs=[
        agg.group(["Color"]),
        agg.avg(["AvgWeightGrams = WeightGrams", "AvgCalories = Calories"]),
    ],
    by="Type",
)
```

If you don't supply the `by` argument to [`agg_by`](../reference/table-operations/group-and-aggregate/aggBy.md), all rows form a single group, and [`agg.group`](../reference/table-operations/group-and-aggregate/AggGroup.md) groups every value of each column you list:

```python test-set=1 order=apples_agg_no_key
apples_agg_no_key = apples.agg_by(
    aggs=agg.group(["Type", "Color", "WeightGrams", "Calories"])
)
```

## `ungroup`

The [`ungroup`](../reference/table-operations/group-and-aggregate/ungroup.md) method is the opposite of [`group_by`](../reference/table-operations/group-and-aggregate/groupBy.md). It expands each [array](../reference/query-language/types/arrays.md) or [vector](../conceptual/arrays-vs-vectors.md) cell into one row per element. With no arguments, `ungroup` expands every array or vector column. When you pass one or more column names, it expands only those columns.

The example below calls [`ungroup`](../reference/table-operations/group-and-aggregate/ungroup.md) with no arguments on `apples_by_class_and_diet`, the table from the [`group_by`](../reference/table-operations/group-and-aggregate/groupBy.md) example above. In the result:

- Each row holds one apple, with a single value in each cell.
- Rows are ordered by group rather than in their original order.
- The `Class` and `Diet` columns remain.

```python test-set=1
new_apples = apples_by_class_and_diet.ungroup()
```

The example below uses [`ungroup`](../reference/table-operations/group-and-aggregate/ungroup.md) to expand only the `Color` column in `apples_by_class_and_diet`. The `Type`, `WeightGrams`, and `Calories` columns still contain [vectors](../conceptual/arrays-vs-vectors.md):

```python test-set=1
apples_ungrouped_by_color = apples_by_class_and_diet.ungroup(["Color"])
```

### Ungroup vectors and Java arrays

The columns you ungroup don't have to come from [`group_by`](../reference/table-operations/group-and-aggregate/groupBy.md). [`ungroup`](../reference/table-operations/group-and-aggregate/ungroup.md) also expands Java arrays that you create in a formula, and it can expand a [vector](../conceptual/arrays-vs-vectors.md) column and an array column in the same call.

The example below uses the [`empty_table`](../reference/table-operations/create/emptyTable.md) method to create a table with two columns and one row. Each column contains a Java array with three elements. The [`ungroup`](../reference/table-operations/group-and-aggregate/ungroup.md) method expands both columns into three rows.

```python order=t,t_ungrouped
from deephaven import empty_table

t = empty_table(1).update(formulas=["X = new int[]{1, 2, 3}", "Z = new int[]{4, 5, 6}"])
t_ungrouped = t.ungroup()
```

The example below creates column `X` as a vector with [`group_by`](../reference/table-operations/group-and-aggregate/groupBy.md) and column `Z` as a Java array. It then uses [`ungroup`](../reference/table-operations/group-and-aggregate/ungroup.md) to expand both columns at the same time.

```python order=t,t_ungrouped
from deephaven import new_table
from deephaven.column import int_col

t = (
    new_table([int_col("X", [1, 2, 3])])
    .group_by()
    .update(formulas=["Z = new int[]{4, 5, 6}"])
)
t_ungrouped = t.ungroup()
```

### Ungroup arrays of different lengths

The [`ungroup`](../reference/table-operations/group-and-aggregate/ungroup.md) method cannot expand a row that contains [arrays](../reference/query-language/types/arrays.md) of different lengths.

The example below creates a table with two columns and one row.

```python test-set=2 order=t
from deephaven import empty_table

t = empty_table(1).update(formulas=["X = new int[]{1, 2, 3}", "Z = new int[]{4, 5}"])
```

Each column in the above table contains a Java array, but one has three elements and the other has two. Since the arrays are not the same size, calling [`ungroup`](../reference/table-operations/group-and-aggregate/ungroup.md) with no column names results in an error.

```python test-set=2 should-fail
t_ungrouped = t.ungroup()  # This results in an error
```

The error message includes `Array sizes differ at row key 0 (position 0), X has size 3, Z has size 2`.

To ungroup [arrays](../reference/query-language/types/arrays.md) of different lengths, ungroup each column separately:

```python test-set=2 order=t_ungrouped_by_x,t_ungrouped_by_z
t_ungrouped_by_x = t.ungroup(["X"])
t_ungrouped_by_z = t.ungroup(["Z"])
```

## Null values

[`group_by`](../reference/table-operations/group-and-aggregate/groupBy.md) keeps null values. Each null becomes an element of the group's [vector](../conceptual/arrays-vs-vectors.md), and the UI displays it as empty. When [`ungroup`](../reference/table-operations/group-and-aggregate/ungroup.md) expands the vector, each null element becomes a null cell in the corresponding column.

The example below uses the [`empty_table`](../reference/table-operations/create/emptyTable.md) method and the [ternary operator](./ternary-if-how-to.md) to create a table with two columns and five rows. The first and second rows of column `Z` contain null values. [`group_by`](../reference/table-operations/group-and-aggregate/groupBy.md) keeps the nulls in the `Z` vector, and [`ungroup`](../reference/table-operations/group-and-aggregate/ungroup.md) turns them back into null cells.

```python order=t,t_by,new_t
from deephaven import empty_table

t = empty_table(5).update(formulas=["X = i", "Z = i < 2 ? NULL_INT : i - 2"])
t_by = t.group_by()
new_t = t_by.ungroup()
```

A cell that holds a null array, rather than an array of nulls, produces no rows when you ungroup it. The example below uses the [`empty_table`](../reference/table-operations/create/emptyTable.md) method to create a table with one column and one row, where the single cell holds a null Java array. Calling [`ungroup`](../reference/table-operations/group-and-aggregate/ungroup.md) on this table results in an empty table with one column.

```python order=t,t_ungrouped
from deephaven import empty_table

t = empty_table(1).update(formulas=["X = (int[])(null)"])
t_ungrouped = t.ungroup()
```

## Related documentation

- [Aggregations in Deephaven](../conceptual/aggregations.md)
- [Create a new table](./new-and-empty-table.md#new_table)
- [Choose the right column selection method](./use-select-view-update.md#choose-the-right-column-selection-method)
- [Formulas in query strings](./formulas.md)
- [Filters in query strings](./filters.md)
- [Operators in query strings](./operators.md)
- [Arrays](../reference/query-language/types/arrays.md)
- [Arrays vs vectors](../conceptual/arrays-vs-vectors.md)
- [Combined aggregations](./combined-aggregations.md)
- [Dedicated aggregations](./dedicated-aggregations.md)
- [`agg_by`](../reference/table-operations/group-and-aggregate/aggBy.md)
- [`agg.group`](../reference/table-operations/group-and-aggregate/AggGroup.md)
- [`empty_table`](../reference/table-operations/create/emptyTable.md)
- [`group_by`](../reference/table-operations/group-and-aggregate/groupBy.md)
- [`new_table`](../reference/table-operations/create/newTable.md)
- [`ungroup`](../reference/table-operations/group-and-aggregate/ungroup.md)
