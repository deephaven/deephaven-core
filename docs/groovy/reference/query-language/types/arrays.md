---
title: Arrays
---

Arrays are an in-memory ordered data structure common in many programming languages. They are typically used to store multiple elements of the same type in an easily iterable manner.

The Deephaven Query Language comes with its own built-in array functionality.

## Usage

### Columns as arrays

DQL allows queries to access a column's array representation by using the `_` suffix on the column name. Use the bracket syntax `[ ]` to access items from a column.

```groovy order=source,result
source = newTable(
    intCol("Values", 1, 2, 3)
)

result = source.update("UpdatedValues = Values_[i] + 1")
```

> [!WARNING]
> The [special variables](../variables/special-variables.md), `i` and `ii`, are unreliable within a ticking table. Inconsistent results occur since previously created row indexes do not automatically update.

[Built-in array methods](/core/javadoc/io/deephaven/vector/Vector.html) like `size()` can be used within queries.

```groovy order=source,result
source = newTable(
    intCol("Values", 1, 2, 3)
)

result = source.update("ValuesSize = Values_.size()")
```

### Convert a column to an array

[Aggregations](../../../how-to-guides/dedicated-aggregations.md) such as [`groupBy`](../../table-operations/group-and-aggregate/groupBy.md) can be used to convert a column into an array. The following example shows how to store a converted array in a table.

```groovy order=source,result
source = newTable(
    intCol("Values", 1, 2, 3)
)

result = source.groupBy()
```

The same bracket syntax and array methods can be used on the resulting array.

Note that `Values` becomes a column containing an array, thus `Values_[0]` is used to access the array.

```groovy order=source,result
source = newTable(
    intCol("Values", 1, 2, 3)
)

result = source.groupBy().update("Length = Values_[0].size()", "FirstElement = Values_[0][0]")
```

### Create a slice or subVector

DQL allows queries to create subVectors or slices of the columns.

```groovy order=source,result
source = newTable(
    doubleCol("X", 1.1, 2.2, 3.3, 4.4, 5.5, 6.6, 7.7, 8.8, 9.9)
)

result = source.update("A= X_.subVector(i-2,i+1)",\
        "rolling_mean = avg(A)", \
        "rolling_median =median(A)", \
        "rolling_sum = sum(A)")
```

## Related documentation

- [How to work with arrays](../../../how-to-guides/work-with-arrays.md)
- [How to use Deephaven's built-in query language functions](../../../how-to-guides/built-in-functions.md)
- [Query language functions](../query-library/query-language-function-reference.md)
- [`newTable`](../../table-operations/create/newTable.md)
- [`update`](../../table-operations/select/update.md)
- [Javadoc](/core/javadoc/io/deephaven/vector/Vector.html)
