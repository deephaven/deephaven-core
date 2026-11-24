---
title: bool_col
---

The `bool_col` function creates a column containing Java `Boolean` values.

> [!NOTE]
> Pass the column that `bool_col` returns to [`new_table`](./newTable.md) to create a table.

## Syntax

```python syntax
bool_col(name: str, data: Sequence) -> InputColumn
```

## Parameters

<ParamTable>
<Param name="name" type="str">

The name of the new column.

</Param>
<Param name="data" type="Sequence">

The column values, as Python `bool` values, such as a list, tuple, NumPy array, or pandas Series. In a list or tuple, `None` values become [null](../../query-language/types/nulls.md).

</Param>
</ParamTable>

## Returns

An [`InputColumn`](./inputColumn.md).

## Example

The following example uses [`new_table`](./newTable.md) to create a table with a single column of boolean values named `Booleans`.

```python
from deephaven import new_table
from deephaven.column import bool_col

result = new_table([bool_col("Booleans", [True, False, True])])
```

## Related documentation

- [Create static tables](../../../how-to-guides/new-and-empty-table.md)
- [`new_table`](./newTable.md)
- [`InputColumn`](./inputColumn.md)
- [Nulls](../../query-language/types/nulls.md)
- [Pydoc](/core/pydoc/code/deephaven.column.html#deephaven.column.bool_col)
