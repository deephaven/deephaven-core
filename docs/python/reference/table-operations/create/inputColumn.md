---
title: InputColumn
---

The `InputColumn` class represents a column of data that can be used as input when creating or updating tables in Deephaven. It is a flexible way to define columns with specific names and data types, supporting a wide range of data sources and types.

> [!NOTE]
> `InputColumn` is often used with [`new_table`](./newTable.md) and related table creation methods to supply data for new columns.

## Syntax

```python syntax
InputColumn(name: str, data: Sequence[Any], dtype: Optional[type] = None) -> InputColumn
```

## Parameters

<ParamTable>
<Param name="name" type="str">
The name of the new column.
</Param>
<Param name="data" type="Sequence[Any]">
The column values. This can be any sequence of compatible data, such as a list, tuple, NumPy ndarray, pandas Series, etc.
</Param>
<Param name="dtype" type="Optional[type]">
(Optional) The data type for the column. If not specified, the type is inferred from the data.
</Param>
</ParamTable>

## Returns

An `InputColumn` object that can be used as an argument to table creation or update methods.

## Examples

```python order=result
from deephaven.column import InputColumn
from deephaven.table_factory import new_table
from deephaven.dtypes import int32

# Create an InputColumn with integer data
ic = InputColumn(name="numbers", data_type=int32, input_data=[1, 2, 3, 4, 5])

# Use InputColumn to create a new table
result = new_table([ic])
```

```python order=result
from deephaven.column import InputColumn
from deephaven.table_factory import new_table
from deephaven.dtypes import double

# Create an InputColumn with integer data
ic = InputColumn(name="floats", data_type=double, input_data=[1.1, 2.2, 3.3])

# Use InputColumn to create a new table
result = new_table([ic])
```

## See Also

- [`bool_col`](./boolCol.md)
- [`new_table`](./newTable.md)
- [Pydoc](https://docs.deephaven.io/core/pydoc/code/deephaven.column.html#deephaven.column.InputColumn)
