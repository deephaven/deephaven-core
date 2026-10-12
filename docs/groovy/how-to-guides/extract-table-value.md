---
title: Extract table values
---

Deephaven provides several ways to extract values from tables into Groovy. Most queries don't need this. It's useful for debugging, for logging, and for code that reads table data directly, such as [table listeners](./table-listeners-groovy.md).

Choose a method based on how you need to find the values:

- To read a value by its position in the table, use [`ColumnVectors`](#columnvectors-positional-index-access). This is the simplest option, and most code should use it.
- To loop over every value in a column, use a [column iterator](#iterate-over-a-column). An iterator reads values in bulk, so it's faster than reading each position separately.
- To read a value by the engine's internal row key, use a [`ColumnSource`](#advanced-columnsource-row-key-access). Use this when your code already has row keys, such as the row sets a table listener receives.

> [!NOTE]
> These methods are for **extracting** values from Deephaven to Groovy, not for **transforming** table data. To create or modify columns, use operations like [`update`](../reference/table-operations/select/update.md). See [Recipes, not loops!](../getting-started/crash-course/vectorization-vs-loops.md) for more details.

## `ColumnVectors` (positional index access)

A row's positional index is its position in the table, counting from 0. The recommended way to extract values by positional index is [`ColumnVectors`](https://deephaven.io/core/javadoc/io/deephaven/engine/table/vectors/ColumnVectors.html). Each `ColumnVectors` method wraps one column in a vector, such as an [`IntVector`](https://deephaven.io/core/javadoc/io/deephaven/vector/IntVector.html) for an `int` column. The vector's `get` method returns the value at a row position.

```groovy order=:log test-set=1
import io.deephaven.engine.table.vectors.ColumnVectors

result = newTable(
    intCol("Integers", 1, 2, 3, 4, 5)
)

// Get value at positional index 1 (second row)
value = ColumnVectors.ofInt(result, "Integers").get(1)
println "Value at index 1: ${value}"
```

Use the method that matches the column's type, such as `ofInt` for an `int` column or `ofObject(table, columnName, type)` for an object column, for example `ColumnVectors.ofObject(t, "Sym", String)`. For the full list of methods, see [The `ColumnVectors` class](./iterate-table-vectors.md#the-columnvectors-class).

> [!WARNING]
> Random access via the vector's `get(position)` method is less efficient than bulk iteration. Use iteration when processing multiple values. See [Iterate over a column](#iterate-over-a-column).

> [!NOTE]
> For a [refreshing (ticking) table](../conceptual/table-types.md), a vector is only valid for the current update cycle. Inside a [listener](./table-listeners-groovy.md), read previous values (each row's values before the current update cycle's changes) by passing `true` as the final `usePreviousValues` argument. For example, `ColumnVectors.ofInt(table, "Integers", true)` reads the previous values of the `Integers` column. For the limits on previous-values vectors, see [Refreshing tables](./iterate-table-vectors.md#refreshing-tables).

## Iterate over a column

To loop over all values in a column, use [`columnIterator`](https://deephaven.io/core/javadoc/io/deephaven/engine/table/Table.html#columnIterator(java.lang.String)). To read primitive values without wrapping each one in a Java object, use a typed variant such as [`integerColumnIterator`](https://deephaven.io/core/javadoc/io/deephaven/engine/table/Table.html#integerColumnIterator(java.lang.String)). If you stop before the iterator is exhausted, call its `close` method to release its resources.

This example loops over the `result` table created in [`ColumnVectors` (positional index access)](#columnvectors-positional-index-access):

```groovy order=:log test-set=1
iterator = result.integerColumnIterator("Integers")

while (iterator.hasNext()) {
    println iterator.nextInt()
}
```

If you already have a vector from [`ColumnVectors`](https://deephaven.io/core/javadoc/io/deephaven/engine/table/vectors/ColumnVectors.html), you can iterate over it directly with its `iterator` method instead. See [Iterate over tables with ColumnVectors](./iterate-table-vectors.md).

## Advanced: `ColumnSource` (row key access)

Most code should use [`ColumnVectors`](#columnvectors-positional-index-access). Use row-key access when your code already has row keys rather than positions, such as the `added` and `modified` row sets a [table listener](./table-listeners-groovy.md) receives.

The [`getColumnSource`](../reference/table-operations/metadata/getColumnSource.md) method returns a [`ColumnSource`](https://deephaven.io/core/javadoc/io/deephaven/engine/table/ColumnSource.html) that provides access to column data by **row key**, not positional index. A row key is the internal identifier the engine assigns to a row. It can differ from the row's positional index, especially in filtered tables or tables whose rows have been added, removed, or shifted. To learn how row keys and positional indices change as tables update, see the [row keys and positional indices FAQ](../reference/community-questions/shifts.md).

```groovy order=:log test-set=2
result = newTable(
    intCol("Integers", 1, 2, 3, 4, 5)
)

columnSource = result.getColumnSource("Integers")
println columnSource.getType()
```

To find the row key for a position, look it up in the table's [`RowSet`](https://deephaven.io/core/javadoc/io/deephaven/engine/rowset/RowSet.html), the ordered set of row keys the table contains. Then read the value with a type-specific method:

```groovy order=:log test-set=2
// Look up the row key of the third row (position 2) from the table's RowSet
rowKey = result.getRowSet().get(2)

// Get the value for that row key using a type-specific method
value = columnSource.getInt(rowKey)
println "Value for row key ${rowKey}: ${value}"
```

Type-specific [`ColumnSource`](https://deephaven.io/core/javadoc/io/deephaven/engine/table/ColumnSource.html) methods:

- `getInt(rowKey)` for `int` columns
- `getLong(rowKey)` for `long` columns
- `getDouble(rowKey)` for `double` columns
- `getFloat(rowKey)` for `float` columns
- `getShort(rowKey)` for `short` columns
- `getByte(rowKey)` for `byte` columns
- `getChar(rowKey)` for `char` columns
- `getBoolean(rowKey)` for `Boolean` columns
- `get(rowKey)` for object columns

`get` also works on primitive columns, but it wraps each value in a Java object, so prefer the type-specific methods for primitives.

> [!NOTE]
> For a refreshing table, values read through a [`ColumnSource`](https://deephaven.io/core/javadoc/io/deephaven/engine/table/ColumnSource.html) are consistent only while no update can change them: inside a [listener](./table-listeners-groovy.md) that listens to the table or [waits for it as a dependency](./table-listeners-groovy.md#dependent-tables), or while your code holds the table's update graph [shared or exclusive lock](../conceptual/query-engine/engine-locking.md#using-locks). Console scripts already hold the exclusive lock, so this matters mainly for code that runs on other threads. See [Engine locking](../conceptual/query-engine/engine-locking.md).

> [!NOTE]
> Inside a listener, use the `getPrev*` methods, such as `getPrevInt`, to read previous values, meaning each row's values before the current update cycle's changes. These methods take a previous row key: one from the update's [`removed`](https://deephaven.io/core/javadoc/io/deephaven/engine/table/TableUpdate.html#removed()) or [`getModifiedPreShift`](https://deephaven.io/core/javadoc/io/deephaven/engine/table/TableUpdate.html#getModifiedPreShift()) row sets, or one you look up in the table's previous `RowSet`, for example `result.getRowSet().prev().get(2)`.

## Related documentation

- [`ColumnVectors`](https://deephaven.io/core/javadoc/io/deephaven/engine/table/vectors/ColumnVectors.html)
- [Iterate over tables with ColumnVectors](./iterate-table-vectors.md)
- [How do row keys and positional indices behave during table operations?](../reference/community-questions/shifts.md)
- [Engine locking](../conceptual/query-engine/engine-locking.md)
- [`getColumnSource`](../reference/table-operations/metadata/getColumnSource.md)
- [`columnIterator`](https://deephaven.io/core/javadoc/io/deephaven/engine/table/Table.html#columnIterator(java.lang.String))
- [`ColumnSource`](https://deephaven.io/core/javadoc/io/deephaven/engine/table/ColumnSource.html)
