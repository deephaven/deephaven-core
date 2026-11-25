---
title: short_col
---

The `short_col` function creates a column containing Java primitive `short` values (16-bit signed integers).

> [!NOTE]
> Pass the column that `short_col` returns to [`new_table`](./newTable.md) to create a table.

> [!IMPORTANT]
> Values must be within the `short` range, -32768 to 32767. Values outside the range wrap around without an error, for example -40000 becomes 25536. The value -32768 (`Short.MIN_VALUE`, available as [`NULL_SHORT`](/core/pydoc/code/deephaven.constants.html#deephaven.constants.NULL_SHORT)) is reserved to represent [null](../../query-language/types/nulls.md) in `short` columns, so -32768, and any value that wraps around to it, becomes null.

## Syntax

```python syntax
short_col(name: str, data: Sequence) -> InputColumn
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

The following example uses [`new_table`](./newTable.md) to create a table with a single column of shorts named `Shorts`.

```python
from deephaven import new_table
from deephaven.column import short_col

result = new_table([short_col("Shorts", [86, 78, 41, 54, 20])])
```

## Related documentation

- [Create static tables](../../../how-to-guides/new-and-empty-table.md)
- [`new_table`](./newTable.md)
- [`InputColumn`](./inputColumn.md)
- [Nulls](../../query-language/types/nulls.md)
- [`deephaven.constants` Pydoc](/core/pydoc/code/deephaven.constants.html)
- [Pydoc](/core/pydoc/code/deephaven.column.html#deephaven.column.short_col)
