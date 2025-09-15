---
title: Arrays
sidebar_label: Arrays
---

This guide shows you how to work with [arrays](../reference/query-language/types/arrays.md) in [query strings](./query-string-overview.md).

[Arrays](../reference/query-language/types/arrays.md) are an invaluable tool for grouping related data. They provide an easy way to access previous and future values in time series datasets. Support for [arrays](../reference/query-language/types/arrays.md) is built into the Deephaven Query Language (DQL).

## Array column types

Array columns fall into one of three categories of data type.

### Array columns

Array columns are Java arrays of primitive types. For example, the following query creates a table with a single row containing an array of primitive integers.

```groovy order=source,sourceMeta
source = emptyTable(1).update("X = new int[]{1, 2, 3}")
sourceMeta = source.meta()
```

You can also use [Groovy closures](./groovy-closures.md) to create Java primitive array columns:

```groovy order=source,sourceMeta
listFunc = { -> [4, 5, 6] }

source = emptyTable(1).update("ArrayFromGroovy = listFunc()")
sourceMeta = source.meta()
```

The Deephaven engine can seamlessly work with these column types.

### Vector columns

```groovy order=source,sourceMeta
source = emptyTable(5).update("X = ii % 2", "Y = ii").groupBy("X")
sourceMeta = source.meta()
```

The Deephaven engine can seamlessly work with these column types.

### Vector columns

Vector columns arise from common table operations including [grouping](./grouping-data.md). These vector columns are used in [dedicated aggregations](./dedicated-aggregations.md), [combined aggregations](./combined-aggregations.md), [`updateBy`](../reference/table-operations/update-by-operations/updateBy.md), and more. The following example creates a vector column by calling [`groupBy`](../reference/table-operations/group-and-aggregate/groupBy.md):

```groovy order=source,sourceMeta
source = emptyTable(5).update("X = ii % 2", "Y = ii").groupBy("X")
sourceMeta = source.meta()
```

## Convert between arrays and vectors

Since Deephaven tables commonly use both Java primitive arrays and Deephaven vectors, it's useful to convert between the two. The following example converts between both vector and array columns:

```groovy order=result,resultMeta,source
source = emptyTable(10).update("Vector = ii").groupBy()
result = source.update(
  "ArrayFromVector = array(Vector)",
  "VectorFromArray = vec(ArrayFromVector)",
)
resultMeta = result.meta()
```

## Create array columns

### By grouping

[Arrays](../reference/query-language/types/arrays.md) can be created using the [`groupBy`](../reference/table-operations/group-and-aggregate/groupBy.md) method to group data.

```groovy order=result,source
source = emptyTable(10).update("X = (ii % 2 == 0) ? `A` : `B` ", "Y = ii")

result = source.groupBy("X")
```

### With aggregations

Certain aggregations create array columns. For example, the following operations create array columns:

- [`RollingGroup`](../reference/table-operations/update-by-operations/rolling-group.md)
- [`rangeJoin`](../reference/table-operations/join/rangeJoin.md)

The following example calls [`RollingGroup`](../reference/table-operations/update-by-operations/rolling-group.md) to create an array column:

```groovy order=result,resultMeta,source
source = emptyTable(10).update(
  "Key = (ii % 2 == 0) ? `A` : `B`", "Value = randomDouble(0, 1)"
)

result = source.updateBy(RollingGroup(3, "TickGroup=Value"), "Key")
resultMeta = result.meta()
```

### Using the underscore operator

Every column in a table has an associated [array](../reference/query-language/types/arrays.md) variable, which can be accessed using the underscore (`_`) operator. This operator is specific to Deephaven. For example, a column called `X` can be accessed as an [array](../reference/query-language/types/arrays.md) by using the column name `X_`:

```groovy order=source,result
source = emptyTable(10).update("X = ii")
result = source.update("A = X_")
```

## Get array length

The [`len`](https://deephaven.io/core/javadoc/io/deephaven/function/Basic.html#len(byte[])) method returns the length of the given input. This is useful in query strings where you need to get the size of a [`Vector`](https://docs.deephaven.io/core/javadoc/io/deephaven/vector/Vector.html) or a Java array.

```groovy order=source,result
source = emptyTable(10).update("X = i").groupBy()
result = source.update("LenX = len(X)")
```

## Access array elements

The square bracket [operators](./operators.md) `[]` are used to access elements in [array](../reference/query-language/types/arrays.md) columns. The following example uses these operators to access the previous and next elements in the array, as well as print the size of the array:

```groovy order=source,result
source = emptyTable(10).update("X = ii")
result = source.update("A = X_[ii - 1]", "B = X_[ii + 1]", "C = X_.size()")
```

> [!NOTE]
> The first row of column `A` is null because there is no previous element at the zeroth array index. The last row of column `B` is null because there is no next element at the last array index.

Additionally, you can access specific [array](../reference/query-language/types/arrays.md) elements directly using indexes.

```groovy order=result,indexingResult
result = (
    emptyTable(10)
    .update("X = (ii % 2 == 0) ? `A` : `B` ", "Y = ii")
    .groupBy("X")
)
indexingResult = result.update("Element2 = Y[2]", "Element3 = Y[3]")
```

## Slice arrays

You can slice [arrays](../reference/query-language/types/arrays.md) into subarrays with [`subVector`](https://deephaven.io/core/javadoc/io/deephaven/engine/table/impl/ssms/LongSegmentedSortedMultiset.html#subVector(long,long)). The first input is the index at which the slice starts, while the second is the index at which the slice ends. They are inclusive and exclusive, respectively.

The following example slices the [array](../reference/query-language/types/arrays.md) column, then [grabs specific elements](#access-array-elements) from the subarray.

```groovy order=result,resultMeta,source
source = (
  emptyTable(10)
  .update("X = (ii % 2 == 0) ? `A` : `B` ", "Y = ii")
  .groupBy("X")
)
result = source.update(
  "SubArray = Y.subVector(2, 4)",
  "SubSlice = SubArray[1]",
)
resultMeta = result.meta()
```

## Functions with array arguments

### Built-in query language functions

> [!CAUTION]
> [Dedicated aggregations](./dedicated-aggregations.md), [Combined aggregations](./combined-aggregations.md), and [`updateBy`](./rolling-calculations.md) are always more performant than [`groupBy`](../reference/table-operations/group-and-aggregate/groupBy.md) followed by manual calculations when used on ticking tables.

Many [built-in query language methods](./built-in-functions.md) take arrays as input. The following example uses the [`sum`](https://docs.deephaven.io/core/javadoc/io/deephaven/function/Numeric.html#sum(io.deephaven.vector.LongVector)) and [`avg`](https://docs.deephaven.io/core/javadoc/io/deephaven/function/Numeric.html#avg(io.deephaven.vector.LongVector)) functions on a column containing [arrays](../reference/query-language/types/arrays.md).

```groovy order=result,sumResult
result = (
  emptyTable(10)
  .update("X = (ii % 2 == 0) ? `A` : `B` ", "Y = ii")
  .groupBy("X")
)
sumResult = result.update("ArraySum = sum(Y)", "ArrayAvg = avg(Y)")
```

### Groovy closures

Groovy closures that take arrays as input are also supported in query strings. The following example calls a Groovy closure that takes a Java array as input:

```groovy order=source,sourceMeta
arraySum = { double[] arr ->
    double total = 0.0
    for (double value : arr) {
        total += value
    }
    return total
}

source = emptyTable(1).update(
    "ArrayColumn = new double[]{3.14, 1.23, -0.919}",
    "CallGroovy = (double)arraySum(ArrayColumn)"
)
sourceMeta = source.meta()
```

## Related documentation

- [Create an empty table](./new-and-empty-table.md#emptytable)
- [Dedicated aggregations](./dedicated-aggregations.md)
- [Combined aggregations](./combined-aggregations.md)
- [Rolling aggregations](./rolling-calculations.md)
- [Query string overview](./query-string-overview.md)
- [Groovy variables in query strings](./groovy-variables.md)
- [Groovy functions in query strings](./groovy-closures.md)
- [Java objects in query strings](./java-classes.md)
- [Special variables](../reference/query-language/variables/special-variables.md)
- [Arrays](../reference/query-language/types/arrays.md)
- [`groupBy`](../reference/table-operations/group-and-aggregate/groupBy.md)
- [`update`](../reference/table-operations/select/update.md)
