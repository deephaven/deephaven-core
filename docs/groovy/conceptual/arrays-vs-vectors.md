---
title: Arrays vs vectors
---

Deephaven tables can store multi-element data in two forms: **Java arrays** and **Deephaven vectors**. Both hold ordered collections of elements, but they differ in ways that affect how you work with them.

You create Java arrays yourself in [formulas](../how-to-guides/formulas.md). Deephaven produces vectors when it [groups rows](../how-to-guides/grouping-data.md) or when a formula reads a whole column through a [column array](../how-to-guides/work-with-arrays.md#using-the-underscore-operator). This page compares the two, shows how to convert between them, and explains when to convert a vector to an array.

## Quick comparison

| Feature                  | Java arrays                                                                                                                                                                           | Deephaven vectors                                                                                                                                                                                                                                                                                                    |
| ------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Type examples**        | `int[]`, `double[]`, `String[]`                                                                                                                                                       | [`IntVector`](https://deephaven.io/core/javadoc/io/deephaven/vector/IntVector.html), [`DoubleVector`](https://deephaven.io/core/javadoc/io/deephaven/vector/DoubleVector.html), [`ObjectVector`](https://deephaven.io/core/javadoc/io/deephaven/vector/ObjectVector.html)                                            |
| **Index type**           | `int` (max ~2.1 billion elements)                                                                                                                                                     | `long` (supports larger datasets)                                                                                                                                                                                                                                                                                    |
| **Out-of-bounds access** | Throws an `ArrayIndexOutOfBoundsException`                                                                                                                                            | Returns the [null value](../reference/query-language/types/nulls.md) for its type (e.g., [`NULL_INT`](../reference/query-language/types/nulls.md))                                                                                                                                                                   |
| **Created by**           | Explicit construction, [`array`](../reference/query-language/query-library/auto-imported/basic.md), or [`arrayObj`](../reference/query-language/query-library/auto-imported/basic.md) | [Table operations that group rows](#deephaven-vectors), [column arrays](../how-to-guides/work-with-arrays.md#using-the-underscore-operator) such as `X_`, [`vec`](../reference/query-language/query-library/auto-imported/basic.md), or [`vecObj`](../reference/query-language/query-library/auto-imported/basic.md) |
| **Memory model**         | Owns contiguous memory                                                                                                                                                                | May be a view into table data                                                                                                                                                                                                                                                                                        |
| **Mutability**           | Java code can reassign elements (`a[i] = v`)                                                                                                                                          | Read-only; no methods to set elements                                                                                                                                                                                                                                                                                |
| **Slicing**              | `Arrays.copyOfRange`                                                                                                                                                                  | [`subVector`](https://deephaven.io/core/javadoc/io/deephaven/vector/Vector.html#subVector(long,long)) (no copy)                                                                                                                                                                                                      |

## Java arrays

Java arrays are standard fixed-size collections. Create them explicitly using Java syntax:

```groovy order=result,resultMeta
result = emptyTable(1).update(
    "IntArray = new int[]{1, 2, 3}",
    "DoubleArray = new double[]{1.1, 2.2, 3.3}",
    "StringArray = new String[]{`a`, `b`, `c`}",
)
resultMeta = result.meta()
```

The metadata shows the columns as `int[]`, `double[]`, and `java.lang.String[]`. A Java array stores its elements itself, and it differs from a vector in three ways:

- Code that receives the array, such as a Groovy closure or Java method, can change an element (`a[i] = v`). A query [formula](../how-to-guides/formulas.md) can't, because formulas can't contain assignments.
- Its index is an `int`, so it holds at most about 2.1 billion elements.
- An out-of-bounds index throws an `ArrayIndexOutOfBoundsException` instead of returning null.

## Deephaven vectors

A Deephaven vector is a collection defined by the [`Vector`](https://deephaven.io/core/javadoc/io/deephaven/vector/Vector.html) interface. Each primitive type except `boolean` has its own subinterface, such as [`IntVector`](https://deephaven.io/core/javadoc/io/deephaven/vector/IntVector.html) and [`DoubleVector`](https://deephaven.io/core/javadoc/io/deephaven/vector/DoubleVector.html). Other types, including `Boolean` and `String`, use [`ObjectVector`](https://deephaven.io/core/javadoc/io/deephaven/vector/ObjectVector.html). Table operations that [group data](../how-to-guides/grouping-data.md) produce vector columns:

```groovy order=result,resultMeta
result = emptyTable(5).update("X = ii % 2", "Y = (int) ii").groupBy("X")
resultMeta = result.meta()
```

The `Y` column is an [`IntVector`](https://deephaven.io/core/javadoc/io/deephaven/vector/IntVector.html), not an `int[]`.

The following operations produce vector columns:

- [`groupBy`](../reference/table-operations/group-and-aggregate/groupBy.md)
- [`AggGroup`](../reference/table-operations/group-and-aggregate/AggGroup.md)
- [`AggDistinct`](../reference/table-operations/group-and-aggregate/AggDistinct.md)
- [`RollingGroup`](../reference/table-operations/update-by-operations/rolling-group.md)
- [`rangeJoin`](../reference/table-operations/join/range-join.md)

A vector that a table operation produces can be a view into the source table's columns. It reads its values from those columns instead of copying them.

This matters for live data. When the source table ticks, each vector that [`groupBy`](../reference/table-operations/group-and-aggregate/groupBy.md) produces reflects its group's rows as of the current update cycle.

### Access a column as a vector

Append `_` to a column name to read the whole column as a vector. Deephaven calls `X_` a [column array](../how-to-guides/work-with-arrays.md#using-the-underscore-operator), but it is a vector that holds every value in column `X`. Index into it to read another row. For example, `X_[ii - 1]` is the value of `X` one row before the current row, where [`ii`](../reference/query-language/variables/special-variables.md) is the current row index:

```groovy order=source,result
source = emptyTable(5).update("X = ii")

result = source.update(
    "PrevValue = X_[ii - 1]",
    "NextValue = X_[ii + 1]",
)
```

The first row's `PrevValue` and the last row's `NextValue` are null because those indices fall outside the column.

Whether a [formula](../how-to-guides/formulas.md) can use a [column array](../how-to-guides/work-with-arrays.md#using-the-underscore-operator) depends on the table:

- On [static tables](./table-types.md#static-tables) and [blink tables](./table-types.md#specialization-3-blink), any column-array expression works.
- On other [refreshing tables](./table-types.md#standard-streaming-tables), a constant offset from the row index, such as `X_[ii - 1]` or `X_[ii + 2]`, works. By default, the engine rejects any other use of a column array.

See [Refreshing table restrictions](../reference/query-language/variables/special-variables.md#refreshing-table-restrictions) for details.

### Key advantages of vectors

**Long-based indexing**: A Java array can't hold more than about 2.1 billion elements, because its index is an `int`. A vector uses a `long` index, so a [group](../how-to-guides/grouping-data.md) or [column array](../how-to-guides/work-with-arrays.md#using-the-underscore-operator) with more rows than that is still a single vector.

**Safe out-of-bounds access**: Reading an index outside a vector returns the [null value](../reference/query-language/types/nulls.md) for the vector's type, such as [`NULL_INT`](../reference/query-language/types/nulls.md) for an [`IntVector`](https://deephaven.io/core/javadoc/io/deephaven/vector/IntVector.html), instead of throwing an exception. The [column-array example](#access-a-column-as-a-vector) shows this: the first row's `PrevValue` is null. The same index on a Java array throws an `ArrayIndexOutOfBoundsException`.

**Efficient slicing**: The [`subVector`](https://deephaven.io/core/javadoc/io/deephaven/vector/Vector.html#subVector(long,long)) method returns another view over the same data, without copying it. The following example uses [`updateView`](../reference/table-operations/select/update-view.md) to keep the slice a view. [`update`](../reference/table-operations/select/update.md) would store a materialized copy of any vector view that a [formula](../how-to-guides/formulas.md) returns:

```groovy order=source,result
source = emptyTable(10).update("X = ii").groupBy()

result = source.updateView(
    "Slice = X.subVector(2, 5)",
    "FirstOfSlice = Slice[0]",
    "PastEnd = Slice[3]",
)
```

`Slice` holds the elements at positions 2, 3, and 4 of `X`, so `FirstOfSlice` is 2. `Slice[3]` is past the end of `Slice`, so `PastEnd` is null.

## Convert between arrays and vectors

Use the built-in [`array`](../reference/query-language/query-library/auto-imported/basic.md) function to convert a vector to an array, and [`vec`](../reference/query-language/query-library/auto-imported/basic.md) to convert an array to a vector:

```groovy order=source,converted,convertedMeta
source = emptyTable(5).update("X = ii").groupBy()

converted = source.update(
    "AsArray = array(X)",
    "BackToVector = vec(AsArray)",
)
convertedMeta = converted.meta()
```

The metadata shows that `AsArray` is a `long[]` and `BackToVector` is a [`LongVector`](https://deephaven.io/core/javadoc/io/deephaven/vector/LongVector.html), the same type as the original `X` column.

### Supported conversions

Each row pairs a primitive array type with its vector type. [`vec`](../reference/query-language/query-library/auto-imported/basic.md) converts the array type to the vector type, and [`array`](../reference/query-language/query-library/auto-imported/basic.md) converts the vector type back.

| Array type | Vector type                                                                               |
| ---------- | ----------------------------------------------------------------------------------------- |
| `byte[]`   | [`ByteVector`](https://deephaven.io/core/javadoc/io/deephaven/vector/ByteVector.html)     |
| `char[]`   | [`CharVector`](https://deephaven.io/core/javadoc/io/deephaven/vector/CharVector.html)     |
| `double[]` | [`DoubleVector`](https://deephaven.io/core/javadoc/io/deephaven/vector/DoubleVector.html) |
| `float[]`  | [`FloatVector`](https://deephaven.io/core/javadoc/io/deephaven/vector/FloatVector.html)   |
| `int[]`    | [`IntVector`](https://deephaven.io/core/javadoc/io/deephaven/vector/IntVector.html)       |
| `long[]`   | [`LongVector`](https://deephaven.io/core/javadoc/io/deephaven/vector/LongVector.html)     |
| `short[]`  | [`ShortVector`](https://deephaven.io/core/javadoc/io/deephaven/vector/ShortVector.html)   |

For object types, use [`arrayObj`](../reference/query-language/query-library/auto-imported/basic.md) to convert an [`ObjectVector`](https://deephaven.io/core/javadoc/io/deephaven/vector/ObjectVector.html) to an array, and [`vecObj`](../reference/query-language/query-library/auto-imported/basic.md) to convert an object array, such as a `String[]`, to an [`ObjectVector`](https://deephaven.io/core/javadoc/io/deephaven/vector/ObjectVector.html).

## When to convert to an array

Deephaven produces vectors for [grouped](../how-to-guides/grouping-data.md) data and [column arrays](../how-to-guides/work-with-arrays.md#using-the-underscore-operator) such as `X_`, so the decision is whether to convert them. In a [formula](../how-to-guides/formulas.md), you usually don't need to. When a formula passes a vector to a Java method that takes an array, the engine converts the vector for you.

In the following example, [`Arrays.stream`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/util/Arrays.html#stream(long%5B%5D)) takes a `long[]`, and the formula passes it the [`LongVector`](https://deephaven.io/core/javadoc/io/deephaven/vector/LongVector.html) column `X` directly:

```groovy order=source,result
source = emptyTable(5).update("X = ii").groupBy()
result = source.update("Total = java.util.Arrays.stream(X).sum()")
```

`Total` is 10, the sum of the values 0 through 4 in `X`, with no call to [`array`](../reference/query-language/query-library/auto-imported/basic.md).

This conversion applies to Java methods only. A Groovy closure receives the vector itself, not an array.

The array that the engine passes can share the vector's data, so the called code must not change it.

**Convert to a Java array yourself when:**

- Code changes elements in place. [`array`](../reference/query-language/query-library/auto-imported/basic.md) and [`arrayObj`](../reference/query-language/query-library/auto-imported/basic.md) return a new copy, so changing it doesn't touch the vector's data. See [Convert between arrays and vectors](#convert-between-arrays-and-vectors).

**Keep Deephaven vectors when:**

- Holding more than about 2.1 billion elements in one collection.
- Reading indices that can fall outside the collection, such as `X_[ii - 1]` on the first row, where you want a null instead of an exception.
- Slicing data without copying.

## Related documentation

- [Deephaven's table types](./table-types.md)
- [Work with arrays](../how-to-guides/work-with-arrays.md)
- [Group and ungroup data](../how-to-guides/grouping-data.md)
- [Formulas](../how-to-guides/formulas.md)
- [Arrays reference](../reference/query-language/types/arrays.md)
- [Convert array types](../reference/community-questions/convert-array-types.md)
- [Nulls](../reference/query-language/types/nulls.md)
- [Special variables and constants](../reference/query-language/variables/special-variables.md)
- [Basic functions](../reference/query-language/query-library/auto-imported/basic.md)
- [`emptyTable`](../reference/table-operations/create/emptyTable.md)
- [`update`](../reference/table-operations/select/update.md)
- [`updateView`](../reference/table-operations/select/update-view.md)
- [`meta`](../reference/table-operations/metadata/meta.md)
- [`groupBy`](../reference/table-operations/group-and-aggregate/groupBy.md)
- [`AggGroup`](../reference/table-operations/group-and-aggregate/AggGroup.md)
- [`AggDistinct`](../reference/table-operations/group-and-aggregate/AggDistinct.md)
- [`RollingGroup`](../reference/table-operations/update-by-operations/rolling-group.md)
- [`rangeJoin`](../reference/table-operations/join/range-join.md)
- [`Vector` Javadoc](/core/javadoc/io/deephaven/vector/Vector.html)
