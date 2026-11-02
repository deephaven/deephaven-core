---
title: Arrays
---

This guide shows you how to work with [arrays](../reference/query-language/types/arrays.md) in [query strings](./query-string-overview.md).

[Arrays](../reference/query-language/types/arrays.md) are an invaluable tool for grouping related data. They provide an easy way to access previous and future values in time-series data, and support for them is built into the Deephaven Query Language (DQL).

## Array column types

On this page, "array column" means a column of [Java arrays](#java-array-columns) or of [Deephaven vectors](#vector-columns). The following sections describe each. "Java array" always means the first kind.

### Java array columns

Java array columns hold Java arrays, most often of primitive types such as `int[]` or `double[]`. For example, the following query creates a table with a single row containing an array of primitive integers.

```groovy order=source,sourceMeta
source = emptyTable(1).update("X = new int[]{1, 2, 3}")
sourceMeta = source.meta()
```

You can also use [Groovy closures](./groovy-closures.md) to create Java primitive array columns. Have the [closure](./groovy-closures.md) return a Java array with `as int[]`. A [closure](./groovy-closures.md) called in a [query string](./query-string-overview.md) returns `Object`, so [cast](./casting.md) the result, as `(int[])listFunc()` does:

```groovy order=source,sourceMeta
listFunc = { -> [4, 5, 6] as int[] }

source = emptyTable(1).update("ArrayFromGroovy = (int[])listFunc()")
sourceMeta = source.meta()
```

Without `as int[]`, the [closure](./groovy-closures.md) returns a Groovy list, so the [cast](./casting.md) fails with a `ClassCastException`. Without the cast, the column is a `java.lang.Object` column, even when the closure returns an `int[]`.

[Built-in query language functions](./built-in-functions.md) such as [`sum`](https://deephaven.io/core/javadoc/io/deephaven/function/Numeric.html) and [`len`](https://deephaven.io/core/javadoc/io/deephaven/function/Basic.html#len(byte[])) accept Java array columns directly. For example, `len(X)` returns 3 for the `X` column in the first example. The same functions work on [vector columns](#vector-columns), as [Get array length](#get-array-length) and [Functions with array arguments](#functions-with-array-arguments) show.

### Vector columns

Vector columns hold Deephaven [vectors](https://deephaven.io/core/javadoc/io/deephaven/vector/Vector.html), such as [`LongVector`](https://deephaven.io/core/javadoc/io/deephaven/vector/LongVector.html) or [`DoubleVector`](https://deephaven.io/core/javadoc/io/deephaven/vector/DoubleVector.html), rather than Java arrays. Operations such as [grouping](./grouping-data.md) produce them. The following example creates a vector column by calling [`groupBy`](../reference/table-operations/group-and-aggregate/groupBy.md):

```groovy order=source,sourceMeta
source = emptyTable(5).update("X = ii % 2", "Y = ii").groupBy("X")
sourceMeta = source.meta()
```

For other operations that produce vector columns, see [Create vector columns](#create-vector-columns). To compare vectors with Java arrays, see [Arrays vs vectors](../conceptual/arrays-vs-vectors.md).

## Create vector columns

Grouping with [`groupBy`](../reference/table-operations/group-and-aggregate/groupBy.md), shown in [Vector columns](#vector-columns), is one way to create vector columns. This section shows two others. [Several operations](#using-aggregations-updateby-and-joins) collect values into vectors, and the [underscore operator](#using-the-underscore-operator) gives a vector view of any column.

### Using aggregations, `updateBy`, and joins

Several operations produce vector columns:

- [`AggGroup`](../reference/table-operations/group-and-aggregate/AggGroup.md)
- [`RollingGroup`](../reference/table-operations/update-by-operations/rolling-group.md)
- [`rangeJoin`](../reference/table-operations/join/range-join.md)

The following example calls [`RollingGroup`](../reference/table-operations/update-by-operations/rolling-group.md) to create a vector column:

```groovy order=result,resultMeta,source
source = emptyTable(10).update(
    "Key = (ii % 2 == 0) ? `A` : `B`", "Value = randomDouble(0, 1)"
)

result = source.updateBy(RollingGroup(3, "TickGroup = Value"), "Key")
resultMeta = result.meta()
```

Each `TickGroup` cell holds the current row's `Value` and up to two earlier `Value`s with the same `Key`, because a [`RollingGroup`](../reference/table-operations/update-by-operations/rolling-group.md) window of 3 ticks includes the current row.

### Using the underscore operator

Every column in a table has an associated column array variable, which holds the column's values as a vector. You refer to it by adding the underscore (`_`) operator to the column name. This syntax is specific to Deephaven. For example, `X_` is the column array variable for column `X`:

```groovy order=source,result
source = emptyTable(10).update("X = ii")
result = source.update("A = X_")
```

Each row of `A` holds the entire `X` column as a vector.

> [!NOTE]
> By default, the engine rejects formulas that use column array variables such as `X_` on [refreshing tables](../conceptual/table-update-model.md), because it can't safely update their results as the table changes. The engine allows two kinds of these formulas anyway:
>
> - Formulas on [blink tables](../conceptual/table-types.md#specialization-3-blink).
> - Formulas on any table that read a constant offset from the current row, such as `X_[ii - 1]`. Here, [`ii`](../reference/query-language/variables/special-variables.md) is the current row index.
>
> See [Refreshing table restrictions](../reference/query-language/variables/special-variables.md#refreshing-table-restrictions) for the full rules and [Alternatives for refreshing tables](./built-in-variables.md#alternatives-for-refreshing-tables) for other approaches.

## Convert between Java arrays and vectors

Since Deephaven tables commonly use both Java primitive arrays and Deephaven vectors, it's useful to convert between the two. Use the built-in [`array`](https://deephaven.io/core/javadoc/io/deephaven/function/Basic.html#array(io.deephaven.vector.LongVector)) function to convert a vector to a Java array, and [`vec`](https://deephaven.io/core/javadoc/io/deephaven/function/Basic.html#vec(long...)) to convert a Java array to a vector. The following example converts a vector column to an array column, then converts that array column back to a vector column:

```groovy order=result,resultMeta,source
source = emptyTable(10).update("Vector = ii").groupBy()
result = source.update(
    "ArrayFromVector = array(Vector)",
    "VectorFromArray = vec(ArrayFromVector)"
)
resultMeta = result.meta()
```

## Get array length

The [`len`](https://deephaven.io/core/javadoc/io/deephaven/function/Basic.html#len(byte[])) function returns the length of its argument. Use it in a [query string](./query-string-overview.md) to get the size of a [`Vector`](https://deephaven.io/core/javadoc/io/deephaven/vector/Vector.html) or a Java array.

```groovy order=source,result
source = emptyTable(10).update("X = i").groupBy()
result = source.update("LenX = len(X)")
```

Vectors also have a [`size`](https://deephaven.io/core/javadoc/io/deephaven/util/datastructures/LongSizedDataStructure.html#size()) method, so `X.size()` returns the same value as `len(X)` in this example.

## Access array elements

Use the square bracket [operators](./operators.md) `[]` to access elements in [array](../reference/query-language/types/arrays.md) columns. The following example indexes `X_`, the column array variable for column `X` described in [Using the underscore operator](#using-the-underscore-operator). It uses the row index [`ii`](../reference/query-language/variables/special-variables.md) to read the elements before and after the current row:

```groovy order=source,result
source = emptyTable(10).update("X = ii")
result = source.update("A = X_[ii - 1]", "B = X_[ii + 1]")
```

> [!NOTE]
> The first row of column `A` is null because `X_[ii - 1]` reads index -1, which is out of bounds. The last row of column `B` is null because `X_[ii + 1]` reads index 10, one past the last element.
>
> On a vector, any out-of-range index returns null, even a negative one. On a Java array, an out-of-range index throws an `ArrayIndexOutOfBoundsException`, so check the index against [`len`](https://deephaven.io/core/javadoc/io/deephaven/function/Basic.html#len(byte[])) first.

To read the same position from every row's [array](../reference/query-language/types/arrays.md), use a constant index. The following example reads the elements at indexes 2 and 3 of each group in the vector column `Y`:

```groovy order=result,indexingResult
result = emptyTable(10)
    .update("X = (ii % 2 == 0) ? `A` : `B`", "Y = ii")
    .groupBy("X")
indexingResult = result.update("Element2 = Y[2]", "Element3 = Y[3]")
```

## Slice arrays

You can slice a vector into a smaller vector with [`subVector`](https://deephaven.io/core/javadoc/io/deephaven/vector/Vector.html#subVector(long,long)). The first argument is the start index, which is inclusive, and the second is the end index, which is exclusive.

To slice a Java array, first convert it to a vector with [`vec`](https://deephaven.io/core/javadoc/io/deephaven/function/Basic.html#vec(long...)), as shown in [Convert between Java arrays and vectors](#convert-between-java-arrays-and-vectors).

The following example slices the vector column `Y`, then [grabs a specific element](#access-array-elements) from the slice.

```groovy order=result,resultMeta,source
source = emptyTable(10)
    .update("X = (ii % 2 == 0) ? `A` : `B`", "Y = ii")
    .groupBy("X")
result = source.update(
    "SubVector = Y.subVector(2, 4)",
    "SubVectorElement = SubVector[1]"
)
resultMeta = result.meta()
```

## Functions with array arguments

### Built-in query language functions

Many [built-in query language functions](./built-in-functions.md) take arrays as input. The following example uses the [`sum`](https://deephaven.io/core/javadoc/io/deephaven/function/Numeric.html#sum(io.deephaven.vector.LongVector)) and [`avg`](https://deephaven.io/core/javadoc/io/deephaven/function/Numeric.html#avg(io.deephaven.vector.LongVector)) functions on a vector column created by [`groupBy`](../reference/table-operations/group-and-aggregate/groupBy.md). The same functions also accept Java array columns.

```groovy order=result,sumResult
result = emptyTable(10)
    .update("X = (ii % 2 == 0) ? `A` : `B`", "Y = ii")
    .groupBy("X")
sumResult = result.update("ArraySum = sum(Y)", "ArrayAvg = avg(Y)")
```

> [!CAUTION]
> On [refreshing tables](../conceptual/table-update-model.md), which update as new data arrives, avoid [`groupBy`](../reference/table-operations/group-and-aggregate/groupBy.md) followed by calculations like the one above. Use [dedicated aggregations](./dedicated-aggregations.md), [combined aggregations](./combined-aggregations.md), or [rolling aggregations with `updateBy`](./rolling-aggregations.md) instead. Their built-in statistics, such as sums and averages, update incrementally. A formula over a grouped column recomputes the whole group each time the group changes.

### Groovy closures

[Query strings](./query-string-overview.md) can also call [Groovy closures](./groovy-closures.md) that take arrays as input. The following example calls a [Groovy closure](./groovy-closures.md) that takes a Java array as input. Because a [closure](./groovy-closures.md) called in a [query string](./query-string-overview.md) returns `Object`, the formula [casts](./casting.md) the result to `double`:

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

- [Arrays vs vectors](../conceptual/arrays-vs-vectors.md)
- [Incremental update model](../conceptual/table-update-model.md)
- [Deephaven's table types](../conceptual/table-types.md)
- [Create an empty table](./new-and-empty-table.md#emptytable)
- [Dedicated aggregations](./dedicated-aggregations.md)
- [Combined aggregations](./combined-aggregations.md)
- [Rolling aggregations](./rolling-aggregations.md)
- [Query string overview](./query-string-overview.md)
- [Groovy variables in query strings](./groovy-variables.md)
- [Groovy closures in queries](./groovy-closures.md)
- [Built-in query language variables](./built-in-variables.md)
- [Built-in query language functions](./built-in-functions.md)
- [Java objects in query strings](./java-classes.md)
- [Group and ungroup data](./grouping-data.md)
- [Casting](./casting.md)
- [Operators](./operators.md)
- [Special variables](../reference/query-language/variables/special-variables.md)
- [Arrays](../reference/query-language/types/arrays.md)
- [`AggGroup`](../reference/table-operations/group-and-aggregate/AggGroup.md)
- [`emptyTable`](../reference/table-operations/create/emptyTable.md)
- [`groupBy`](../reference/table-operations/group-and-aggregate/groupBy.md)
- [`meta`](../reference/table-operations/metadata/meta.md)
- [`rangeJoin`](../reference/table-operations/join/range-join.md)
- [`RollingGroup`](../reference/table-operations/update-by-operations/rolling-group.md)
- [`update`](../reference/table-operations/select/update.md)
- [`updateBy`](../reference/table-operations/update-by-operations/updateBy.md)
- [`Vector` Javadoc](/core/javadoc/io/deephaven/vector/Vector.html)
- [`Basic` Javadoc](/core/javadoc/io/deephaven/function/Basic.html)
- [`Numeric` Javadoc](/core/javadoc/io/deephaven/function/Numeric.html)
