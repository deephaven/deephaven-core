---
title: Extract table values
---

Deephaven tables have methods to extract values from tables into the native programming language. Generally, this isn't necessary for Deephaven queries but may be useful for debugging and logging purposes, and other specific use cases such as using listeners.

## ColumnVectors (positional index access)

The recommended way to extract values from a table by positional index is using [`ColumnVectors`](https://deephaven.io/core/javadoc/io/deephaven/engine/table/vectors/ColumnVectors.html). This provides random access to column data by row position.

```groovy order=:log test-set=1
import io.deephaven.engine.table.vectors.ColumnVectors

result = newTable(
    intCol("Integers", 1, 2, 3, 4, 5)
)

// Get value at positional index 1 (second row)
value = ColumnVectors.ofInt(result, "Integers").get(1)
println "Value at index 1: ${value}"
```

For different data types, use the appropriate `ColumnVectors` method:

- `ColumnVectors.ofInt()` for `int` columns
- `ColumnVectors.ofLong()` for `long` columns
- `ColumnVectors.ofDouble()` for `double` columns
- `ColumnVectors.ofFloat()` for `float` columns
- `ColumnVectors.ofObject()` for object columns

> [!WARNING]
> Random access via `ColumnVectors.get(position)` is less efficient than bulk iteration. Use iteration when processing multiple values.

## table.getColumnSource() (row key access)

The [`getColumnSource`](https://deephaven.io/core/javadoc/io/deephaven/engine/table/Table.html#getColumnSource(java.lang.String)) method returns a [`ColumnSource`](https://deephaven.io/core/javadoc/io/deephaven/engine/table/ColumnSource.html) that provides access to column data by **row key**, not positional index.

```groovy order=:log test-set=2
result = newTable(
    intCol("Integers", 1, 2, 3, 4, 5)
)

columnSource = result.getColumnSource("Integers")
println columnSource
```

> [!IMPORTANT] > `ColumnSource` methods like `get(rowKey)` use **row keys**, not positional indices. Row keys are the internal identifiers for rows and may not match positional indices, especially in filtered or modified tables.

For primitive columns, use type-specific methods for better performance:

```groovy order=:log test-set=2
// Get value for row key 2 using type-specific method
value = columnSource.getInt(2)
println "Value for row key 2: ${value}"
```

Type-specific `ColumnSource` methods:

- `getInt(rowKey)` for `int` columns
- `getLong(rowKey)` for `long` columns
- `getDouble(rowKey)` for `double` columns
- `getFloat(rowKey)` for `float` columns
- `get(rowKey)` for object columns

## table.columnIterator()

To loop over all values in a column, use [`columnIterator`](https://deephaven.io/core/javadoc/io/deephaven/engine/table/Table.html#columnIterator(java.lang.String)). This is the most efficient way to process all values.

```groovy order=:log test-set=1
iterator = result.columnIterator("Integers")

while (iterator.hasNext()) {
    println iterator.next()
}
```

## Related Documentation

- [`ColumnVectors`](https://deephaven.io/core/javadoc/io/deephaven/engine/table/vectors/ColumnVectors.html)
- [Iterate over tables](iterate-table-vectors.md)
- [How do row keys and positional indices behave?](https://deephaven.io/core/docs/reference/community-questions/shifts/)
- [`getColumnSource`](https://deephaven.io/core/javadoc/io/deephaven/engine/table/Table.html#getColumnSource(java.lang.String))
- [`columnIterator`](https://deephaven.io/core/javadoc/io/deephaven/engine/table/Table.html#columnIterator(java.lang.String))
- [`ColumnSource`](https://deephaven.io/core/javadoc/io/deephaven/engine/table/ColumnSource.html)
