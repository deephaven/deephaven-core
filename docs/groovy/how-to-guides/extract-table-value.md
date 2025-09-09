---
title: Extract table values
---

Deephaven tables have methods to extract values from tables into the native programming language. Generally, this isn't necessary for Deephaven queries but may be useful for debugging and logging purposes, and other specific use cases such as using listeners.

## table.getColumnSource()

The general syntax to extract a specific value from a table is `value = table.getColumnSource("column").get(index)`.

The [`getColumnSource`](https://deephaven.io/core/javadoc/io/deephaven/engine/table/Table.html#getColumnSource(java.lang.String)) method allows you to convert a column to a [`ColumnSource`](/core/javadoc/io/deephaven/engine/table/ColumnSource.html) object.

```groovy order=:log test-set=1
result = newTable(
    intCol("Integers", 1, 2, 3, 4, 5)
)

columnSource = result.getColumnSource("Integers")

println columnSource
```

Once you've gotten your column source, you can use its methods. For extracting a table value, you use the [`get`](https://deephaven.io/core/javadoc/io/deephaven/engine/table/ElementSource.html#get(long)) method to retrieve the value of the column at the given index.

```groovy order=:log test-set=1
println columnSource.get(2)
```

## table.columnIterator()

In cases where you want to loop over all the values in a column, you can use the [`columnIterator`](https://deephaven.io/core/javadoc/io/deephaven/engine/table/Table.html#columnIterator(java.lang.String)) method. This method returns an iterator for all the values in the column.

```groovy order=:log test-set=1
iterator = result.columnIterator("Integers")

while (iterator.hasNext()) {
    println iterator.next()
}
```

## Related Documentation

- [`getColumnSource`](https://deephaven.io/core/javadoc/io/deephaven/engine/table/Table.html#getColumnSource(java.lang.String))
- [`columnIterator`](https://deephaven.io/core/javadoc/io/deephaven/engine/table/Table.html#columnIterator(java.lang.String))
- [`ColumnSource`](/core/javadoc/io/deephaven/engine/table/ColumnSource.html)
