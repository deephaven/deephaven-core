---
title: Extract table values
---

Deephaven provides several ways to extract values from tables into Python. Most queries don't need this. It's useful for debugging, for logging, and for code that reads table data directly, such as [table listeners](./table-listeners-python.md).

Choose a method based on how you need to find the values:

- To read a value by its position in the table, use [`to_numpy`](#to_numpy-positional-index-access). It copies the selected columns into a NumPy array, so it suits small tables or a few columns.
- To loop over every value in a column, or over several columns of different types, use a [`for` loop](#iterate-with-for-loops) over a NumPy array or a table iterator. Table iterators read the table in chunks instead of copying whole columns into memory.
- To read a value by the engine's internal row key, use a [`ColumnSource`](#advanced-columnsource-row-key-access). This advanced option calls the Java API directly, so use it only when you need row keys rather than positions.

> [!NOTE]
> These methods are for **extracting** values from Deephaven to Python, not for **transforming** table data. To create or modify columns, use operations like [`update`](../reference/table-operations/select/update.md). See [Recipes, not loops!](../getting-started/crash-course/vectorization-vs-loops.md) for more details.

## `to_numpy` (positional index access)

A row's positional index is its position in the table, counting from 0. The recommended way to extract values by positional index is [`to_numpy`](../reference/numpy/to-numpy.md). It converts the selected columns into a 2D NumPy array with one array column per table column. You then read values from the array by index.

```python order=result test-set=1
from deephaven import empty_table
from deephaven.numpy import to_numpy

result = empty_table(5).update(["Integers = i + 1"])
```

To extract a single value by positional index, convert the column to a NumPy array, select its only column as a one-dimensional array, and read the value by index:

```python order=:log test-set=1
# Convert column to numpy array
integers_array = to_numpy(result, cols=["Integers"])[:, 0]

# Access value at positional index 1 (second row)
value = integers_array[1]
print(f"Value at index 1: {value}")
```

You can also use array slicing to extract multiple values:

```python order=:log test-set=1
# Get first three values
first_three = integers_array[0:3]
print(f"First three values: {first_three}")
```

> [!WARNING]
> [`to_numpy`](../reference/numpy/to-numpy.md) copies every row of the selected columns into memory, and for a [refreshing (ticking) table](../conceptual/table-types.md) it first takes a snapshot of the whole table. For large tables, keep only the columns you need with [`view`](../reference/table-operations/select/view.md), and reduce the rows with an operation such as [`where`](../reference/table-operations/filter/where.md) or [`head`](../reference/table-operations/filter/head.md), before converting. Because the snapshot is a table operation, don't call `to_numpy` on a refreshing table inside a [table listener](./table-listeners-python.md). Use a table iterator, such as [`iter_tuple`](../reference/data-import-export/iterate/iter-tuple.md), instead.

> [!IMPORTANT]
> All columns that [`to_numpy`](../reference/numpy/to-numpy.md) converts must have the same data type. If you omit `cols`, it converts every column in the table, so every column must have the same type. For mixed types, convert columns separately, or loop over rows with a table iterator. [Iterate with for loops](#iterate-with-for-loops) shows both.

## Iterate with `for` loops

To loop over a column's values, convert the column to a NumPy array with [`to_numpy`](../reference/numpy/to-numpy.md), then use a standard Python `for` loop. This example loops over `integers_array`, created in [`to_numpy` (positional index access)](#to_numpy-positional-index-access):

```python order=:log test-set=1
# Iterate over all values
for value in integers_array:
    print(value)
```

To loop over several columns at once, one row at a time, the next examples use a table with two `int` columns and one `String` column:

```python order=multi_col_table test-set=2
from deephaven import empty_table
from deephaven.numpy import to_numpy

multi_col_table = empty_table(5).update(
    ["X = i + 1", "Y = (i + 1) * 10", "Z = (i % 2 == 0) ? `Even` : `Odd`"]
)
```

[`to_numpy`](../reference/numpy/to-numpy.md) can convert columns of the same data type together into one 2D array. Convert columns of different data types separately:

```python order=:log test-set=2
# X and Y are both int columns, so convert them together
xy_array = to_numpy(multi_col_table, cols=["X", "Y"])

# Z is a String column, so convert it separately
z_array = to_numpy(multi_col_table, cols=["Z"])[:, 0]

# Each row of xy_array holds one X value and one Y value
for (x_val, y_val), z_val in zip(xy_array, z_array):
    print(f"X: {x_val}, Y: {y_val}, Z: {z_val}")
```

To avoid copying whole columns into memory at once, or to loop over columns of different data types together, use a table iterator such as [`iter_tuple`](../reference/data-import-export/iterate/iter-tuple.md) or [`iter_dict`](../reference/data-import-export/iterate/iter-dict.md). Table iterators read the table in chunks of rows. See [Table iterators](./iterate-table-data.md) for details.

```python order=:log test-set=2
# Each row is a named tuple with one field per column
for row in multi_col_table.iter_tuple(cols=["X", "Y", "Z"]):
    print(f"X: {row.X}, Y: {row.Y}, Z: {row.Z}")
```

## Advanced: `ColumnSource` (row key access)

For advanced use cases, you can access the underlying Java [`ColumnSource`](https://deephaven.io/core/javadoc/io/deephaven/engine/table/ColumnSource.html) through the table's `j_object` attribute, which holds the Java table that the Python [`Table`](https://deephaven.io/core/pydoc/code/deephaven.table.html#deephaven.table.Table) wraps. This bypasses the Python API and isn't idiomatic Python, so use [`to_numpy`](../reference/numpy/to-numpy.md) for most use cases.

A [`ColumnSource`](https://deephaven.io/core/javadoc/io/deephaven/engine/table/ColumnSource.html) provides access by **row key**, not positional index. A row key is the internal identifier the engine assigns to a row. It can differ from the row's positional index, especially in filtered tables or tables whose rows have been added, removed, or shifted. To learn how row keys and positional indices change as tables update, see the [row keys and positional indices FAQ](../reference/community-questions/shifts.md).

```python order=:log test-set=3
from deephaven import empty_table

result = empty_table(5).update(["Integers = i + 1"])

# Access the underlying Java ColumnSource
column_source = result.j_object.getColumnSource("Integers")
print(column_source.getType())
```

To find the row key for a position, look it up in the table's [`RowSet`](https://deephaven.io/core/javadoc/io/deephaven/engine/rowset/RowSet.html), the ordered set of row keys the table contains. Then read the value with a type-specific method:

```python order=:log test-set=3
# Look up the row key of the third row (position 2) from the table's RowSet
row_key = result.j_object.getRowSet().get(2)

# Get the value for that row key using a type-specific method
value = column_source.getInt(row_key)
print(f"Value for row key {row_key}: {value}")
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
> Unlike [`to_numpy`](../reference/numpy/to-numpy.md), which takes a snapshot of a refreshing table before copying it, a [`ColumnSource`](https://deephaven.io/core/javadoc/io/deephaven/engine/table/ColumnSource.html) reads the table's live data. For a refreshing table, values read this way are consistent only while no update can change them: inside a [listener](./table-listeners-python.md), or inside a [`shared_lock`](https://deephaven.io/core/pydoc/code/deephaven.update_graph.html#deephaven.update_graph.shared_lock) or [`exclusive_lock`](https://deephaven.io/core/pydoc/code/deephaven.update_graph.html#deephaven.update_graph.exclusive_lock) block. Console scripts already hold the exclusive lock, so this matters mainly for code that runs on other threads. See [Engine locking](../conceptual/query-engine/engine-locking.md).

> [!NOTE]
> Inside a listener, use the `getPrev*` methods, such as `getPrevInt`, to read previous values, meaning each row's values before the current update cycle's changes. These methods take a previous row key, which you look up in the table's previous `RowSet`, for example `result.j_object.getRowSet().prev().get(2)`.

## Related documentation

- [`to_numpy`](../reference/numpy/to-numpy.md)
- [Use NumPy in Deephaven queries](./use-numpy.md)
- [Table iterators](./iterate-table-data.md)
- [How do row keys and positional indices behave during table operations?](../reference/community-questions/shifts.md)
- [Engine locking](../conceptual/query-engine/engine-locking.md)
- [`ColumnSource` Javadoc](https://deephaven.io/core/javadoc/io/deephaven/engine/table/ColumnSource.html)
