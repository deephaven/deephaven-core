---
title: Extract table values
---

Deephaven tables have methods to extract values from tables into Python. Generally, this isn't necessary for Deephaven queries but may be useful for debugging and logging purposes, and other specific use cases such as using listeners.

## to_numpy() (positional index access)

The recommended way to extract values from a table by positional index is using [`to_numpy`](../reference/numpy/to-numpy.md). This converts table columns to NumPy arrays, which provide positional index access.

```python order=result test-set=1
from deephaven import empty_table
from deephaven.numpy import to_numpy

result = empty_table(5).update(["Integers = i + 1"])
```

To extract a single value by positional index, convert the column to a NumPy array and access by index:

```python order=:log test-set=1
# Convert column to numpy array
integers_array = to_numpy(result, cols=["Integers"])

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
> `to_numpy` copies the entire table into memory. For large tables, consider limiting table size before converting.

> [!IMPORTANT]
> All columns passed to `to_numpy` must have the same data type. For mixed types, convert columns individually.

## Iterate with for loops

For iteration, you can use standard Python for loops with NumPy arrays:

```python order=:log test-set=1
# Iterate over all values
for value in integers_array:
    print(value)
```

For multiple columns, convert each separately or use table operations:

```python order=multi_col_table test-set=2
from deephaven import empty_table
from deephaven.numpy import to_numpy

multi_col_table = empty_table(5).update(
    ["X = i + 1", "Y = (i + 1) * 10", "Z = (i % 2 == 0) ? `Even` : `Odd`"]
)
```

```python order=:log test-set=2
# Convert numeric columns
x_array = to_numpy(multi_col_table, cols=["X"])
y_array = to_numpy(multi_col_table, cols=["Y"])

# Iterate over multiple arrays together
for x_val, y_val in zip(x_array, y_array):
    print(f"X: {x_val}, Y: {y_val}")
```

## Advanced: ColumnSource (row key access)

For advanced use cases, you can access the underlying Java [`ColumnSource`](https://deephaven.io/core/javadoc/io/deephaven/engine/table/ColumnSource.html) via `.j_object`. This provides access by **row key**, not positional index.

```python order=:log test-set=3
from deephaven import empty_table

result = empty_table(5).update(["Integers = i + 1"])

# Access the underlying Java ColumnSource
column_source = result.j_object.getColumnSource("Integers")
print(column_source)
```

> [!IMPORTANT] 
> `ColumnSource` methods use **row keys**, not positional indices. Row keys are internal identifiers that may not match positional indices, especially in filtered or modified tables.

For primitive columns, use type-specific methods:

```python order=:log test-set=3
# Get value for row key 2 using type-specific method
value = column_source.getInt(2)
print(f"Value for row key 2: {value}")
```

Type-specific `ColumnSource` methods:

- `getInt(rowKey)` for `int` columns
- `getLong(rowKey)` for `long` columns
- `getDouble(rowKey)` for `double` columns
- `getFloat(rowKey)` for `float` columns
- `get(rowKey)` for object columns

> [!WARNING]
> Using `.j_object` directly accesses the Java layer and is not idiomatic Python. Use `to_numpy()` for most use cases.

## Related documentation

- [`to_numpy`](../reference/numpy/to-numpy.md)
- [NumPy integration](./use-numpy.md)
- [How do row keys and positional indices behave?](https://deephaven.io/core/docs/reference/community-questions/shifts/)
- [`ColumnSource` Javadoc](https://deephaven.io/core/javadoc/io/deephaven/engine/table/ColumnSource.html)
