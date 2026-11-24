---
title: float_col
---

The `float_col` function creates a column containing Java primitive `float` values (32-bit floating-point numbers).

> [!NOTE]
> Pass the column that `float_col` returns to [`new_table`](./newTable.md) to create a table.

> [!IMPORTANT]
> The value `-Float.MAX_VALUE` (available as [`NULL_FLOAT`](/core/pydoc/code/deephaven.constants.html#deephaven.constants.NULL_FLOAT)) is reserved to represent [null](../../query-language/types/nulls.md) in `float` columns. [NaN](../../query-language/types/NaNs.md) and infinite values are stored as-is, not as null.

## Syntax

```python syntax
float_col(name: str, data: Sequence) -> InputColumn
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

The following example uses [`new_table`](./newTable.md) to create a table with a single column of floats named `Floats`.

```python
from deephaven import new_table
from deephaven.column import float_col

result = new_table([float_col("Floats", [9.9, 8.8, 7.7])])
```

## Related documentation

- [Create static tables](../../../how-to-guides/new-and-empty-table.md)
- [`new_table`](./newTable.md)
- [`InputColumn`](./inputColumn.md)
- [NaNs](../../query-language/types/NaNs.md)
- [Nulls](../../query-language/types/nulls.md)
- [`deephaven.constants` Pydoc](/core/pydoc/code/deephaven.constants.html)
- [Pydoc](/core/pydoc/code/deephaven.column.html#deephaven.column.float_col)
