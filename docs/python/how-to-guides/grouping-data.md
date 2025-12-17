---
title: Group and ungroup data
---

This guide will show you how to group and ungroup table data in Deephaven.

This guide uses a table of apple data called `apples` created using [`new_table`](../reference/table-operations/create/newTable.md). Many of the grouping and ungrouping examples use this table. If you are unfamiliar with creating tables from scratch using [`new_table`](../reference/table-operations/create/newTable.md), please see our guide on [creating new tables](./new-and-empty-table.md#new_table).

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

The [`group_by`](../reference/table-operations/group-and-aggregate/groupBy.md) method groups columnar data into [arrays](../reference/query-language/types/arrays.md). A list of grouping column names defines grouping keys. All rows from the input table with the same key values are grouped together. The values in the arrays for each group in the output table maintain their order from the input table.

If no input is supplied to [`group_by`](../reference/table-operations/group-and-aggregate/groupBy.md), then there will be one group, which contains all of the data. The resultant table will contain a single row, where column data is grouped into a single [array](../reference/query-language/types/arrays.md). This is shown in the example below:

```python test-set=1
apples_by_no_column = apples.group_by()
```

If a single input is supplied to [`group_by`](../reference/table-operations/group-and-aggregate/groupBy.md), then the resultant table will have row data grouped into [arrays](../reference/query-language/types/arrays.md) based on each unique value in the input column. This is shown in the example below:

```python test-set=1
apples_by_type = apples.group_by(by=["Type"])
```

If more than one input is supplied to [`group_by`](../reference/table-operations/group-and-aggregate/groupBy.md), then the resultant table will have row data grouped into [arrays](../reference/query-language/types/arrays.md) based on unique value pairs from the grouping columns. This is shown in the example below:

```python test-set=1
apples_by_type_and_color = apples.group_by(by=["Type", "Color"])
```

If you want to group data by conditions on columns, you can do so by using [the appropriate table update method](../how-to-guides/use-select-view-update.md#choose-the-right-column-selection-method) (in this case, it's [`update_view`](../reference/table-operations/select/update-view.md)) to update the table before grouping:

```python test-set=1
apples_by_class_and_diet = apples.update_view(
    formulas=[
        "Class = (WeightGrams < 90) ? `Light` : `Heavy`",
        "Diet = (Calories < 50) ? `Allowed` : `Not Allowed`",
    ]
).group_by(["Class", "Diet"])
```

## `agg.group`

The [`agg.group`](../reference/table-operations/group-and-aggregate/AggGroup.md) method returns an aggregator that computes an array of all values within an aggregation group, for each column. Like the other aggregation methods, it is used in conjunction with the [`agg_by`](../reference/table-operations/group-and-aggregate/aggBy.md) method.

> [!NOTE]
> Unlike [`group_by`](../reference/table-operations/group-and-aggregate/groupBy.md), [`agg.group`](../reference/table-operations/group-and-aggregate/AggGroup.md) throws an error if you don't supply any column names.

In this example, we will group `Color`, `WeightGrams`, and `Calories` by `Type`:

```python test-set=1 order=apples_by_type
from deephaven import agg

apples_by_type = apples.agg_by(
    aggs=agg.group(["WeightGrams", "Calories", "Color"]), by="Type"
)
```

If the `by` parameter is not supplied, the `agg.group` method will group all the values from each column:

```python test-set=1 order=apples_by_no_column_2
apples_by_no_column_2 = apples.agg_by(
    aggs=agg.group(["Type", "Color", "WeightGrams", "Calories"])
)
```

## Ungroup data with `ungroup`

The [`ungroup`](../reference/table-operations/group-and-aggregate/ungroup.md) method is the opposite of [`group_by`](../reference/table-operations/group-and-aggregate/groupBy.md). It expands content from [arrays](../reference/query-language/types/arrays.md) or vectors into columns of singular values and builds a new set of rows from it. The method takes optional columns as input. If no inputs are supplied, all [array](../reference/query-language/types/arrays.md) or vector columns are expanded. If one or more columns are given as input, only those columns will have their [array](../reference/query-language/types/arrays.md) values expanded into new rows.

The example below shows how [`ungroup`](../reference/table-operations/group-and-aggregate/ungroup.md) reverses the [`group_by`](../reference/table-operations/group-and-aggregate/groupBy.md) operation used to create `apples_by_class_and_diet` when no columns are given as input. Notice how all [array](../reference/query-language/types/arrays.md) columns have been expanded, leaving a single element in each row of the resultant table:

```python test-set=1
new_apples = apples_by_class_and_diet.ungroup()
```

The example below uses [`ungroup`](../reference/table-operations/group-and-aggregate/ungroup.md) to expand the `Color` column in `apples_by_class_and_diet`. This expands only [arrays](../reference/query-language/types/arrays.md) in the `Color` column, and not the others. Notice how the `Type`, `WeightGrams`, and `Calories` columns still contain [arrays](../reference/query-language/types/arrays.md):

```python test-set=1
apples_ungrouped_by_color = apples_by_class_and_diet.ungroup(["Color"])
```

## Different array types

The [`ungroup`](../reference/table-operations/group-and-aggregate/ungroup.md) method can ungroup DbArrays and Java arrays.

The example below uses the [`empty_table`](../reference/table-operations/create/emptyTable.md) method to create a table with two columns and one row. Each column contains a Java array with 3 elements. The [`ungroup`](../reference/table-operations/group-and-aggregate/ungroup.md) method works as expected on Java arrays.

```python order=t,t_ungrouped
from deephaven import empty_table

t = empty_table(1).update(formulas=["X = new int[]{1, 2, 3}", "Z = new int[]{4, 5, 6}"])
t_ungrouped = t.ungroup()
```

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

## Handling different array lengths

The [`ungroup`](../reference/table-operations/group-and-aggregate/ungroup.md) method cannot unpack a row that contains [arrays](../reference/query-language/types/arrays.md) of different length.

To demonstrate this, we'll start by creating a table with two columns and one row.

```python test-set=2 order=t
from deephaven import empty_table

t = empty_table(1).update(formulas=["X = new int[]{1, 2, 3}", "Z = new int[]{4, 5}"])
```

Each column in the above table contains a Java array, but one has three elements and the other has two. Since the arrays are not the same size, calling [`ungroup`](../reference/table-operations/group-and-aggregate/ungroup.md) without an input column will result in an error.

```python test-set=2 should-fail
t_ungrouped = t.ungroup()  # This results in an error
```

![A collapsed error message highlighted in the Deephaven IDE](../assets/how-to/t_ungrouped_Error.png)

It is only possible to ungroup columns of the same length. [Arrays](../reference/query-language/types/arrays.md) of different lengths must be ungrouped separately.

```python test-set=2 order=t_ungrouped_by_x,t_ungrouped_by_z
t_ungrouped_by_x = t.ungroup(["X"])
t_ungrouped_by_z = t.ungroup(["Z"])
```

## Null values

Using [`group_by`](../reference/table-operations/group-and-aggregate/groupBy.md) on a table with null values will work properly. Null values will appear as empty [array](../reference/query-language/types/arrays.md) elements when grouped with [`group_by`](../reference/table-operations/group-and-aggregate/groupBy.md). Null [array](../reference/query-language/types/arrays.md) elements expanded using [`ungroup`](../reference/table-operations/group-and-aggregate/ungroup.md) will appear as null (empty) row entries in the corresponding column.

The example below uses the [`empty_table`](../reference/table-operations/create/emptyTable.md) method and the [ternary operator](./ternary-if-how-to.md) to create a table with two columns of 5 rows. The first and second rows contain null values. Null values behave as expected during grouping and ungrouping.

```python order=t,t_by,new_t
from deephaven import empty_table
from deephaven.constants import NULL_INT

t = empty_table(5).update(formulas=["X = i", "Z = i < 2 ? NULL_INT : i-2"])
t_by = t.group_by()
new_t = t_by.ungroup()
```

The example below uses the [`empty_table`](../reference/table-operations/create/emptyTable.md) method to create a table with one column and one row. The single cell in the table contains a null Java array. Calling [`ungroup`](../reference/table-operations/group-and-aggregate/ungroup.md) on this table results in an empty table with one column.

```python order=t,t_ungrouped
from deephaven import empty_table

t = empty_table(1).update(formulas=["X = (int[])(null)"])
t_ungrouped = t.ungroup()
```

## Use of grouping in table operations

Many Deephaven table operations use grouping internally. For example, [`agg_by`](../reference/table-operations/group-and-aggregate/aggBy.md) creates groups specified by the key column(s) given in the `by` parameter. The grouping is done automatically, and the resultant table shows summary statistics calculated for each group.

Table operations that require grouping do the grouping internally. It is always more performant to use these table operations than to group data first and then apply some calculations over the groups.

## Related documentation

- [Create a new table](./new-and-empty-table.md#new_table)
- [Choose the right selection method](./use-select-view-update.md#choose-the-right-column-selection-method)
- [Formulas in query strings](./formulas.md)
- [Filters in query strings](./filters.md)
- [Operators in query strings](./operators.md)
- [Arrays](../reference/query-language/types/arrays.md)
- [`empty_table`](../reference/table-operations/create/emptyTable.md)
- [`group_by`](../reference/table-operations/group-and-aggregate/groupBy.md)
- [`new_table`](../reference/table-operations/create/newTable.md)
- [`ungroup`](../reference/table-operations/group-and-aggregate/ungroup.md)
