---
title: Group and ungroup data
---

This guide will show you how to group and ungroup table data in Deephaven.

This guide uses a table of apple data called `apples` created using [`newTable`](../reference/table-operations/create/newTable.md). Many of the grouping and ungrouping examples use this table. If you are unfamiliar with creating tables from scratch using [`newTable`](../reference/table-operations/create/newTable.md), please see our guide [Create a new table](./new-and-empty-table.md#newtable).

Use the code below to create the `apples` table:

```groovy test-set=1
apples = newTable(
    stringCol("Type", "Granny Smith", "Granny Smith", "Gala", "Gala", "Golden Delicious", "Golden Delicious"),
    stringCol("Color", "Green", "Green", "Red-Green", "Orange-Green", "Yellow", "Yellow"),
    intCol("WeightGrams", 102, 85, 79, 92, 78, 99),
    intCol("Calories", 53, 48, 51, 61, 46, 57)
)
```

## Group data with `groupBy`

[`groupBy`](../reference/table-operations/group-and-aggregate/groupBy.md) groups columnar data into [arrays](../reference/query-language/types/arrays.md). A list of grouping column names defines grouping keys. All rows from the input table with the same key values are grouped together.

If no input is supplied to [`groupBy`](../reference/table-operations/group-and-aggregate/groupBy.md), then there will be one group, which contains all of the data. The resultant table will contain a single row, where column data is grouped into a single [array](../reference/query-language/types/arrays.md). This is shown in the example below:

```groovy test-set=1
applesByNoColumn = apples.groupBy()
```

If a single input is supplied to [`groupBy`](../reference/table-operations/group-and-aggregate/groupBy.md), then the resultant table will have row data grouped into [arrays](../reference/query-language/types/arrays.md) based on each unique value in the input column. This is shown in the example below:

```groovy test-set=1
applesByType = apples.groupBy("Type")
```

If more than one input is supplied to [`groupBy`](../reference/table-operations/group-and-aggregate/groupBy.md), then the resultant table will have row data grouped into [arrays](../reference/query-language/types/arrays.md) based on unique value pairs from the grouping columns. This is shown in the example below:

```groovy test-set=1
applesByTypeAndColor = apples.groupBy("Type", "Color")
```

If you want to group data by conditions on columns, you can do so by using [the appropriate table update method](../how-to-guides/use-select-view-update.md#choose-the-right-column-selection-method) (in this case, it's [`updateView`](../reference/table-operations/select/update-view.md)) to update the table before grouping:

```groovy test-set=1
applesByClassAndDiet = apples.updateView(
    "Class = (WeightGrams < 90) ? `Light` : `Heavy`",
    "Diet = (Calories < 50) ? `Allowed` : `Not Allowed`")
    .groupBy("Class", "Diet")
```

## Ungroup data with `ungroup`

The [`ungroup`](../reference/table-operations/group-and-aggregate/ungroup.md) method is the reverse of [`groupBy`](../reference/table-operations/group-and-aggregate/groupBy.md). It expands content from [arrays](../reference/query-language/types/arrays.md) or vectors and builds a new set of rows from it. The method takes optional columns as input. If no inputs are supplied, all [array](../reference/query-language/types/arrays.md) or vector columns are expanded. If one or more columns are given as input, only those columns will have their [array](../reference/query-language/types/arrays.md) values expanded into new rows.

The example below shows how [`ungroup`](../reference/table-operations/group-and-aggregate/ungroup.md) reverses the [`groupBy`](../reference/table-operations/group-and-aggregate/groupBy.md) operation used to create `applesByClassAndDiet` when no columns are given as input. Notice how all [array](../reference/query-language/types/arrays.md) columns have been expanded, leaving a single element in each row of the resultant table:

```groovy test-set=1
newApples = applesByClassAndDiet.ungroup()
```

The example below uses [`ungroup`](../reference/table-operations/group-and-aggregate/ungroup.md) to expand the `Color` column in `applesByClassAndDiet`. This expands only [arrays](../reference/query-language/types/arrays.md) in the `Color` column, and not the others. Notice how the `Type`, `WeightGrams`, and `Calories` columns still contain [arrays](../reference/query-language/types/arrays.md):

```groovy test-set=1
applesUngroupedByColor = applesByClassAndDiet.ungroup("Color")
```

## Different array types

The [`ungroup`](../reference/table-operations/group-and-aggregate/ungroup.md) method can ungroup DbArrays and Java arrays.

The example below uses the [`emptyTable`](../reference/table-operations/create/emptyTable.md) method to create a table with two columns and one row. Each column contains a Java array with 3 elements. The [`ungroup`](../reference/table-operations/group-and-aggregate/ungroup.md) method works as expected on Java arrays.

```groovy order=t,t_ungrouped
t = emptyTable(1).update("X = new int[]{1, 2, 3}", "Z = new int[]{4, 5, 6}")
t_ungrouped = t.ungroup()
```

The example below uses [`ungroup`](../reference/table-operations/group-and-aggregate/ungroup.md) to unwrap both an Array (column `X`) and a Java array (column `Z`) at the same time.

```groovy order=t,t_ungrouped
t = newTable(
    intCol("X", 1, 2, 3)
).groupBy().update("Z = new int[]{4, 5, 6}")

t_ungrouped = t.ungroup()
```

## Different array lengths

The [`ungroup`](../reference/table-operations/group-and-aggregate/ungroup.md) method cannot unpack a row that contains [arrays](../reference/query-language/types/arrays.md) of different length.

The example below uses the [`emptyTable`](../reference/table-operations/create/emptyTable.md) method to create a table with two columns and one row. Each column contains a Java array, but one has three elements and the other has two. Calling [`ungroup`](../reference/table-operations/group-and-aggregate/ungroup.md) without an input column will result in an error.

```groovy skip-test
t = emptyTable(1).update("X = new int[]{1, 2, 3}", "Z = new int[]{4, 5}")
t_ungrouped = t.ungroup() // This results in an error
```

![The above table with a different array length in each column](../assets/how-to/t_diffArrayLengths.png)
![The error message generated by Deephaven upon running the above `t_ungrouped`](../assets/how-to/t_ungrouped_Error.png)

It is only possible to ungroup columns of the same length. [Arrays](../reference/query-language/types/arrays.md) of different lengths must be ungrouped separately.

```groovy order=t,t_ungroupedByX,t_ungroupedByZ
t = emptyTable(1).update("X = new int[]{1, 2, 3}", "Z = new int[]{4, 5}")
t_ungroupedByX = t.ungroup("X")
t_ungroupedByZ = t.ungroup("Z")
```

## Null values

Using [`groupBy`](../reference/table-operations/group-and-aggregate/groupBy.md) on a table with null values will work properly. Null values will appear as empty [array](../reference/query-language/types/arrays.md) elements when grouped with [`groupBy`](../reference/table-operations/group-and-aggregate/groupBy.md). Null [array](../reference/query-language/types/arrays.md) elements unwrapped using [`ungroup`](../reference/table-operations/group-and-aggregate/ungroup.md) will appear as null (empty) row entries in the corresponding column.

The example below uses the [`emptyTable`](../reference/table-operations/create/emptyTable.md) method and the [ternary operator](../how-to-guides/ternary-if-how-to.md) to create a table with two columns of 5 rows. The first and second rows contain null values. Null values behave as expected during grouping and ungrouping.

```groovy order=t,t_by,new_t
t = emptyTable(5).update("X = i", "Z = i < 2 ? NULL_INT : i-2")
t_by = t.groupBy()
new_t = t_by.ungroup()
```

The example below uses the [`emptyTable`](../reference/table-operations/create/emptyTable.md) method to create a table with one column and one row. The single cell in the table contains a null Java array. Calling [`ungroup`](../reference/table-operations/group-and-aggregate/ungroup.md) on this table results in an empty table with one column.

```groovy order=t,t_ungrouped
t = emptyTable(1).update("X = (int[])(null)")
t_ungrouped = t.ungroup()
```

## Related documentation

- [Create new and empty tables](./new-and-empty-table.md)
- [Choose the right selection method](../how-to-guides/use-select-view-update.md#choose-the-right-column-selection-method)
- [Arrays](../reference/query-language/types/arrays.md)
- [`emptyTable`](../reference/table-operations/create/emptyTable.md)
- [`groupBy`](../reference/table-operations/group-and-aggregate/groupBy.md)
- [`newTable`](../reference/table-operations/create/newTable.md)
- [ternary-if](../how-to-guides/ternary-if-how-to.md)
- [`ungroup`](../reference/table-operations/group-and-aggregate/ungroup.md)
