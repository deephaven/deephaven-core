---
title: int_col
---

The `int_col` function creates a column containing Java primitive `int` values (32-bit signed integers).

> [!NOTE]
> Pass the column that `int_col` returns to [`new_table`](./newTable.md) to create a table.

> [!IMPORTANT]
> Values must be within the `int` range, -2147483648 to 2147483647. Values outside the range wrap around without an error, for example 2147483648 becomes -2147483648. The value -2147483648 (`Integer.MIN_VALUE`, available as [`NULL_INT`](/core/pydoc/code/deephaven.constants.html#deephaven.constants.NULL_INT)) is reserved to represent [null](../../query-language/types/nulls.md) in `int` columns, so -2147483648, and any value that wraps around to it, becomes null. `int` columns do not support infinite or not-a-number (NaN) values.

## Syntax

```python syntax
int_col(name: str, data: Sequence) -> InputColumn
```

## Parameters

<ParamTable>
<Param name="name" type="str">

The name of the new column.

</Param>
<Param name="data" type="Sequence">

The column values, such as a list, tuple, NumPy array, or pandas Series. `None` values become null.

</Param>
</ParamTable>

## Returns

An [`InputColumn`](./inputColumn.md).

## Example

The following example uses [`new_table`](./newTable.md) to create a table with a single column of integers named `Integers`.

```python
from deephaven import new_table
from deephaven.column import int_col

result = new_table([int_col("Integers", [1, 2, 3, 4, 5])])
```

## Related documentation

- [Create static tables](../../../how-to-guides/new-and-empty-table.md)
- [`new_table`](./newTable.md)
- [`InputColumn`](./inputColumn.md)
- [Nulls](../../query-language/types/nulls.md)
- [`deephaven.constants` Pydoc](/core/pydoc/code/deephaven.constants.html)
- [Pydoc](/core/pydoc/code/deephaven.column.html#deephaven.column.int_col)
