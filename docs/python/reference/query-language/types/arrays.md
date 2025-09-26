---
title: Arrays
---

Arrays are an in-memory ordered data structure common in many programming languages. They are typically used to store multiple elements of the same type in an easily iterable manner. Arrays in the Deephaven engine are implemented by [Vector](/core/javadoc/io/deephaven/vector/Vector.html) and its subclasses.

The Deephaven Query Language comes with its own built-in array functionality.

## Usage

### Columns as arrays

DQL allows queries to access a column's array representation by using the `_` suffix on the column name. Use the bracket
syntax `[]` to access items from a column.

```python order=source,result
from deephaven import new_table
from deephaven.column import int_col

source = new_table([int_col("Values", [1, 2, 3])])

result = source.update(formulas=["UpdatedValues = Values_[i] + 1"])
```

> [!WARNING]
> The [special variables](../variables/special-variables.md), `i` and `ii`, are unreliable within a ticking table.
> Inconsistent results occur since previously created row indexes do not automatically update.

[Built-in array methods](/core/javadoc/io/deephaven/vector/Vector.html) like `size()` can be used
within queries.

```python order=source,result
from deephaven import new_table
from deephaven.column import int_col

source = new_table([int_col("Values", [1, 2, 3])])

result = source.update(formulas=["ValuesSize = Values_.size()"])
```

### Convert a column to an array

[Aggregations](../../../how-to-guides/dedicated-aggregations.md) such
as [`group_by`](../../table-operations/group-and-aggregate/groupBy.md) can be used to convert a column into an array.
The following example shows how to store a converted array in a table.

```python order=source,result
from deephaven import new_table
from deephaven.column import int_col

source = new_table([int_col("Values", [1, 2, 3])])
result = source.group_by()
```

The same bracket syntax and array methods can be used on the resulting array.

Note that `Values` becomes a column containing an array, thus `Values_[0]` is used to access the array.

```python order=source,result
from deephaven import new_table
from deephaven.column import int_col

source = new_table([int_col("Values", [1, 2, 3])])

result = source.group_by().update(
    formulas=["Length = Values_[0].size()", "FirstElement = Values_[0][0]"]
)
```

### Create a slice or subVector

DQL allows queries to create subVectors or slices of the columns.

```python order=source,result
from deephaven import new_table
from deephaven.column import double_col

source = new_table([double_col("X", [1.1, 2.2, 3.3, 4.4, 5.5, 6.6, 7.7, 8.8, 9.9])])

result = source.update(
    formulas=[
        "A= X_.subVector(i-2,i+1)",
        "rolling_mean = avg(A)",
        "rolling_median =median(A)",
        "rolling_sum = sum(A)",
    ]
)
```

## Related documentation

- [How to work with arrays](../../../how-to-guides/work-with-arrays.md)
- [Built-in query language constants](../../../how-to-guides/built-in-constants.md)
- [Built-in query language variables](../../../how-to-guides/built-in-variables.md)
- [Built-in query language functions](../../../how-to-guides/built-in-functions.md)
- [`new_table`](../../table-operations/create/newTable.md)
- [`update`](../../table-operations/select/update.md)
- [Javadoc](/core/javadoc/io/deephaven/vector/Vector.html)
- [Pydoc](/core/pydoc/code/deephaven.dtypes.html#deephaven.dtypes.array)
