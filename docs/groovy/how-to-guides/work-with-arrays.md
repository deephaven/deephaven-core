---
title: Work with arrays
sidebar_label: Arrays
---

This guide will show you how to work with [arrays](../reference/query-language/types/arrays.md) in your query strings.

When performing complex analyses, [arrays](../reference/query-language/types/arrays.md) are an invaluable tool. [Arrays](../reference/query-language/types/arrays.md) group related data together and provide an easy way to access offset data from a time series. [Arrays](../reference/query-language/types/arrays.md) are built into the Deephaven Query Language.

## Create an array from a column

Every column in a table has an associated [array](../reference/query-language/types/arrays.md) variable, which can be accessed by adding an underscore after the column name. For example, a column called `X` can be accessed as an [array](../reference/query-language/types/arrays.md) by using the column name `X_`.

In the following example, the data in column `X` can be accessed through the `X_` [array](../reference/query-language/types/arrays.md).

```groovy order=source,result
source = emptyTable(10).update("X = ii")
result = source.update("A = X_")
```

Let's walk through the query step-by-step.

- First, we create a simple table with ten rows, each populated with a numeric value ranging from 0 to 9.
  - Variable `ii` is a [special, built-in variable](../reference/query-language/variables/special-variables.md) that contains the row number and is useful for accessing [arrays](../reference/query-language/types/arrays.md).
  - As a consequence, each row in column `X` contains a value that indicates its index within the column. For example, the first row has a value of 0 while the fifth row has a value of 4.
- Next, we create a new column `A`, using the data from the first column accessed as an [array](../reference/query-language/types/arrays.md). Since `A` accessed as an [array](../reference/query-language/types/arrays.md) contains its values from each row, this puts a 10-element [array](../reference/query-language/types/arrays.md) in each row for column B.

> [!WARNING]
> The [special variables](../reference/query-language/variables/special-variables.md), `i` and `ii`, are unreliable within a ticking table. Inconsistent results occur since previously created row indexes do not automatically update.

## Access array elements

You can use bracket `[ ]` syntax to access elements in the [array](../reference/query-language/types/arrays.md) at specific indexes for your queries. The [special variables](../reference/query-language/variables/special-variables.md) `i` and `ii` can be useful for indexing based on row number.

In the following example, we access various elements from the data in column `X` to add columns to the result table.

```groovy order=source,result
source = emptyTable(10).update("X = ii")
result = source.update("A = X_[ii - 1]", "B = X_[ii + 1]", "C = X_.size()")
```

- Column `X` is referred to as `X_` to access it as an [array](../reference/query-language/types/arrays.md), and then the values of specific rows within the column are accessed by using `X_[ii-1]` and `X_[ii+1]`.
- Columns `A` and `B` are created as offset values of column `X` by using this [array](../reference/query-language/types/arrays.md) access.
- Column `C` contains the size of column `X`, defined by the `X_.size()` function.
- Variable `ii` is a [special, built-in variable](../reference/query-language/variables/special-variables.md) that contains the row number and is useful for accessing [arrays](../reference/query-language/types/arrays.md).
- When you index an out-of-bounds element in the [array](../reference/query-language/types/arrays.md), you get a null result.

## Create arrays by grouping

[Arrays](../reference/query-language/types/arrays.md) can also be created using the [`groupBy`](../reference/table-operations/group-and-aggregate/groupBy.md) method to group data.

```groovy order=source,result
source = emptyTable(10).update("X = (ii % 2 == 0) ? `A` : `B` ", "Y = ii")

result = source.groupBy("X")
```

- The source table contains two columns:
  - `X` with alternating values of `A` and `B` in each row,
  - `Y` containing each row's index.
- The result table uses the [`groupBy`](../reference/table-operations/group-and-aggregate/groupBy.md) operation to group based on the values in column X. The resulting grouped column contains [arrays](../reference/query-language/types/arrays.md) of values.

## Access specific array elements

In the following example, we start by creating the result table from the previous example, then access specific [array](../reference/query-language/types/arrays.md) indexes from the `Y` column.

```groovy order=result,indexingResult
result = emptyTable(10).update("X = (ii % 2 == 0) ? `A` : `B` ", "Y = ii").groupBy("X")
indexingResult = result.update(
    "Element2=Y[2]",
    "Element3=Y[3]"
)
```

## Slice arrays

You can grab slices of an [array](../reference/query-language/types/arrays.md) by using the [`Array.subVector(start, end)`](https://deephaven.io/core/javadoc/io/deephaven/engine/table/impl/ssms/LongSegmentedSortedMultiset.html#subVector(long,long)) method. `start` and `end` are both integers showing where the slice starts (inclusive) and ends (exclusive).

In the following example, we can make subarrays and also access specific elements from the subarrays.

```groovy order=result,slice
result = emptyTable(10).update("X = (ii % 2 == 0) ? `A` : `B` ", "Y = ii").groupBy("X")
slice = result.update(
    "SubArray = Y.subVector(2, 4)",
    "SubSlice = SubArray[1]",
)
```

## Use aggregations on arrays

You can use aggregation functions on [arrays](../reference/query-language/types/arrays.md). The following example uses the `sum` and `avg` functions on a column containing [arrays](../reference/query-language/types/arrays.md).

```groovy order=result,sumResult
result = emptyTable(10).update("X = (ii % 2 == 0) ? `A` : `B` ", "Y = ii").groupBy("X")
sumResult = result.update("ArraySum = sum(Y)", "ArrayAvg = avg(Y)")
```

## Get array length

The [`len`](https://deephaven.io/core/javadoc/io/deephaven/function/Basic.html#len(byte[])) method returns the length of the given input. This is useful in query strings where a user needs to get the size of a `Vector` or a Java array.

```groovy order=source,result
source = emptyTable(10).update("X = i").groupBy()
result = source.update("LenX = len(X)")
```

Use the power of [arrays](../reference/query-language/types/arrays.md) in the Deephaven Query Language to make your queries more powerful and concise.

## Related documentation

- [Create an empty table](./new-and-empty-table.md#emptytable)
- [Arrays](../reference/query-language/types/arrays.md)
- [`groupBy`](../reference/table-operations/group-and-aggregate/groupBy.md)
- [Special variables](../reference/query-language/variables/special-variables.md)
- [`update`](../reference/table-operations/select/update.md)
