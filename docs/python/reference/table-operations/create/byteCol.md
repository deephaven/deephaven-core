---
title: byte_col
---

The `byte_col` function creates a column containing Java primitive `byte` values (8-bit signed integers).

> [!NOTE]
> Pass the column that `byte_col` returns to [`new_table`](./newTable.md) to create a table.

> [!IMPORTANT]
> Values must be within the `byte` range, -128 to 127. Values outside the range wrap around without an error, for example 300 becomes 44. The value -128 (`Byte.MIN_VALUE`, available as [`NULL_BYTE`](/core/pydoc/code/deephaven.constants.html#deephaven.constants.NULL_BYTE)) is reserved to represent [null](../../query-language/types/nulls.md) in `byte` columns, so -128, and any value that wraps around to it, becomes null.

## Syntax

```python syntax
byte_col(name: str, data: Sequence) -> InputColumn
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

The following example uses [`new_table`](./newTable.md) to create a table with a single column of bytes named `Bytes`.

```python
from deephaven import new_table
from deephaven.column import byte_col

result = new_table([byte_col("Bytes", [1, 2, 3, 4])])
```

## Related documentation

- [Create static tables](../../../how-to-guides/new-and-empty-table.md)
- [`new_table`](./newTable.md)
- [`InputColumn`](./inputColumn.md)
- [Nulls](../../query-language/types/nulls.md)
- [`deephaven.constants` Pydoc](/core/pydoc/code/deephaven.constants.html)
- [Pydoc](/core/pydoc/code/deephaven.column.html#deephaven.column.byte_col)
