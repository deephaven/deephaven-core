---
title: InputColumn
---

The `InputColumn` class represents a column of data that can be used as input when creating or updating tables in Deephaven. It is a flexible way to define columns with specific names and data types, supporting a wide range of data sources and types.

> [!NOTE]
> `InputColumn` is often used with [`new_table`](./newTable.md) and related table creation methods to supply data for new columns.

## Syntax

```python syntax
InputColumn(
    name: str,
    data_type: DType,
    component_type: DType = None,
    column_type: ColumnType = ColumnType.NORMAL,
    input_data: Any = None,
) -> InputColumn
```

## Parameters

<ParamTable>
<Param name="name" type="str">
The name of the new column.
</Param>
<Param name="data_type" type="DType">
The column data type.
</Param>
<Param name="component_type" type="DType" optional>
The column component type. The default is `None`.
</Param>
<Param name="column_type" type="ColumnType" optional>
The column type: `ColumnType.NORMAL` or `ColumnType.PARTITIONING`. The default is `ColumnType.NORMAL`.
</Param>
<Param name="input_data" type="Any" optional>
The column values, such as a list, tuple, NumPy ndarray, or pandas Series. The default is `None`, which creates an empty column.
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

# Create an InputColumn with floating-point data
ic = InputColumn(name="floats", data_type=double, input_data=[1.1, 2.2, 3.3])

# Use InputColumn to create a new table
result = new_table([ic])
```

## See Also

- [`bool_col`](./boolCol.md)
- [`new_table`](./newTable.md)
- [Pydoc](https://docs.deephaven.io/core/pydoc/code/deephaven.column.html#deephaven.column.InputColumn)
