---
title: long_col
---

The `long_col` function creates a column containing Java primitive `long` values (64-bit signed integers).

> [!NOTE]
> Pass the column that `long_col` returns to [`new_table`](./newTable.md) to create a table.

> [!IMPORTANT]
> The value -9223372036854775808 (`Long.MIN_VALUE`, available as [`NULL_LONG`](/core/pydoc/code/deephaven.constants.html#deephaven.constants.NULL_LONG)) is reserved to represent [null](../../query-language/types/nulls.md) in `long` columns. A Python `int` outside the 64-bit range raises a `DHError` instead of wrapping around. `long` columns do not support infinite or not-a-number (NaN) values.

## Syntax

```python syntax
long_col(name: str, data: Sequence) -> InputColumn
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

The following example uses [`new_table`](./newTable.md) to create a table with a single column of longs named `Longs`.

```python
from deephaven import new_table
from deephaven.column import long_col

result = new_table([long_col("Longs", [10000000, 9876543210, -314159265])])
```

## Related documentation

- [Create static tables](../../../how-to-guides/new-and-empty-table.md)
- [`new_table`](./newTable.md)
- [`InputColumn`](./inputColumn.md)
- [Nulls](../../query-language/types/nulls.md)
- [`deephaven.constants` Pydoc](/core/pydoc/code/deephaven.constants.html)
- [Pydoc](/core/pydoc/code/deephaven.column.html#deephaven.column.long_col)
