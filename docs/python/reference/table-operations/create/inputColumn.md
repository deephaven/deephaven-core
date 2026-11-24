---
title: InputColumn
---

The `InputColumn` class represents a user-defined column with a name, a data type, and optional input data. Pass a list of `InputColumn` objects to [`new_table`](./newTable.md) to create an in-memory table.

The `*_col` helper functions, such as [`int_col`](./intCol.md), [`double_col`](./doubleCol.md), and [`string_col`](./stringCol.md), return `InputColumn` objects and are simpler to use because each one sets the data type itself. Use `InputColumn` directly for data types that no helper function creates, such as array types, `dtypes.LocalDate`, `dtypes.LocalTime`, and `dtypes.BigDecimal`. `LocalDate`, `LocalTime`, and `BigDecimal` columns need Java objects as input data, because jpy does not convert Python `datetime.date`, `datetime.time`, or `decimal.Decimal` values to these types. Create `LocalDate` and `LocalTime` values with [`to_j_local_date`](../../time/datetime/to_j_local_date.md) and [`to_j_local_time`](../../time/datetime/to_j_local_time.md).

## Syntax

```python syntax
InputColumn(
    name: str,
    data_type: DType,
    component_type: Optional[DType] = None,
    column_type: ColumnType = ColumnType.NORMAL,
    input_data: Optional[Any] = None,
) -> InputColumn
```

## Parameters

<ParamTable>
<Param name="name" type="str">
The name of the new column.
</Param>
<Param name="data_type" type="DType">
The column data type. Pass a [`deephaven.dtypes.DType`](../../python/deephaven-python-types.md) value, such as `dtypes.int32` or `dtypes.int32_array`. See [Python data types](../../python/deephaven-python-types.md) for the available types.
</Param>
<Param name="component_type" type="Optional[DType]" optional>
The column component type for array and vector columns. The default is `None`, which infers the component type from `data_type`, such as `dtypes.int32` for `dtypes.int32_array`. [`new_table`](./newTable.md) sets each column's type from `data_type` only, so this value does not change the table it returns. A `component_type` that does not match `data_type`, such as `dtypes.int64` for `dtypes.int32_array`, raises a `DHError`.
</Param>
<Param name="column_type" type="ColumnType" optional>
The column type, a [`deephaven.column.ColumnType`](/core/pydoc/code/deephaven.column.html#deephaven.column.ColumnType): `ColumnType.NORMAL` or `ColumnType.PARTITIONING`. The default is `ColumnType.NORMAL`. [`new_table`](./newTable.md) creates every column as a normal column, so this value does not change the table it returns.
</Param>
<Param name="input_data" type="Optional[Any]" optional>
The column values, such as a list, tuple, NumPy ndarray, or pandas Series. The default is `None`, which creates an empty column. [`new_table`](./newTable.md) requires every column to have the same number of values.
</Param>
</ParamTable>

## Returns

An `InputColumn` to pass to [`new_table`](./newTable.md).

## Examples

The following example creates an `int` column and a `double` column, and then passes both to [`new_table`](./newTable.md):

```python order=result
from deephaven import dtypes, new_table
from deephaven.column import InputColumn

# Create an InputColumn with integer data
ints = InputColumn(name="Numbers", data_type=dtypes.int32, input_data=[1, 2, 3])

# Create an InputColumn with double data
doubles = InputColumn(
    name="Doubles", data_type=dtypes.double, input_data=[1.1, 2.2, 3.3]
)

# Use the InputColumns to create a new table
result = new_table([ints, doubles])
```

The following example creates an `int[]` array column, which none of the `*_col` helper functions can create. Each element of `input_data` is one row's value, a Java `int` array created with [`dtypes.array`](/core/pydoc/code/deephaven.dtypes.html#deephaven.dtypes.array):

```python order=result
from deephaven import dtypes, new_table
from deephaven.column import InputColumn
import numpy as np

# Create Java int arrays to use as the values of two rows
row1 = dtypes.array(dtypes.int32, np.array([1, 2, 3], dtype=np.int32))
row2 = dtypes.array(dtypes.int32, np.array([4, 5], dtype=np.int32))

# Create an InputColumn with array data, one array per row
int_array_col = InputColumn(
    name="IntArrayCol", data_type=dtypes.int32_array, input_data=[row1, row2]
)

result = new_table([int_array_col])
```

## Related documentation

- [Create static tables](../../../how-to-guides/new-and-empty-table.md)
- [Python data types](../../python/deephaven-python-types.md)
- [`bool_col`](./boolCol.md)
- [`double_col`](./doubleCol.md)
- [`int_col`](./intCol.md)
- [`new_table`](./newTable.md)
- [`string_col`](./stringCol.md)
- [`to_j_local_date`](../../time/datetime/to_j_local_date.md)
- [`to_j_local_time`](../../time/datetime/to_j_local_time.md)
- [`ColumnType` Pydoc](/core/pydoc/code/deephaven.column.html#deephaven.column.ColumnType)
- [`dtypes.array` Pydoc](/core/pydoc/code/deephaven.dtypes.html#deephaven.dtypes.array)
- [Pydoc](/core/pydoc/code/deephaven.column.html#deephaven.column.InputColumn)
