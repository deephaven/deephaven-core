---
title: bool_col
---

The `bool_col` method creates a column containing Java primitive boolean values.

> [!NOTE]
> This method is commonly used with [`new_table`](./newTable.md) to create tables.

## Syntax

```python syntax
bool_col(name: str, data: Sequence[bool]) -> InputColumn
```

## Parameters

<ParamTable>
<Param name="name" type="str">

The name of the new column.

</Param>
<Param name="data" type="Sequence[bool]">

The column values. This can be any sequence of compatible data, e.g., list, tuple, ndarray, pandas series, etc.

</Param>
</ParamTable>

## Returns

An [`InputColumn`](/core/pydoc/code/deephaven.column.html#deephaven.column.InputColumn).

## Example

The following examples use [`new_table`](./newTable.md) to create a table with a single column of boolean values.

```python
from deephaven import new_table
from deephaven.column import bool_col

result = new_table([bool_col("Booleans", [True, False, True])])
```

## Related Documentation

- [Create a new table](../../../how-to-guides/new-and-empty-table.md#new_table)
- [`new_table`](./newTable.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/engine/util/TableTools.html#booleanCol(java.lang.String,java.lang.Boolean...))
- [Pydoc](/core/pydoc/code/deephaven.column.html#deephaven.column.bool_col)
