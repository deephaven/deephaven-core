---
title: double_col
---

The `double_col` function creates a column containing Java primitive `double` values (64-bit floating-point numbers).

> [!NOTE]
> Pass the column that `double_col` returns to [`new_table`](./newTable.md) to create a table.

> [!IMPORTANT]
> The value `-Double.MAX_VALUE` (available as [`NULL_DOUBLE`](/core/pydoc/code/deephaven.constants.html#deephaven.constants.NULL_DOUBLE)) is reserved to represent [null](../../query-language/types/nulls.md) in `double` columns. [NaN](../../query-language/types/NaNs.md) and infinite values are stored as-is, not as null.

## Syntax

```python syntax
double_col(name: str, data: Sequence) -> InputColumn
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

The following example uses [`new_table`](./newTable.md) to create a table with a single column of doubles named `Doubles`.

```python
from deephaven import new_table
from deephaven.column import double_col

result = new_table([double_col("Doubles", [0.1, 0.2, 0.3])])
```

## Related documentation

- [Create static tables](../../../how-to-guides/new-and-empty-table.md)
- [`new_table`](./newTable.md)
- [`InputColumn`](./inputColumn.md)
- [NaNs](../../query-language/types/NaNs.md)
- [Nulls](../../query-language/types/nulls.md)
- [`deephaven.constants` Pydoc](/core/pydoc/code/deephaven.constants.html)
- [Pydoc](/core/pydoc/code/deephaven.column.html#deephaven.column.double_col)
