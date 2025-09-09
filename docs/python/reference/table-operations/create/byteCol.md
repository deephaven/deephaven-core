---
title: byte_col
---

The `byte_col` method creates a column containing Java primitive byte values.

> [!NOTE]
> This method is commonly used with [`new_table`](./newTable.md) to create tables.

## Syntax

```python syntax
byte_col(name: str, data: Sequence[byte]) -> InputColumn
```

## Parameters

<ParamTable>
<Param name="name" type="str">

The name of the new column.

</Param>
<Param name="data" type="Sequence[byte]">

The column values. This can be any sequence of compatible data, e.g., list, tuple, ndarray, pandas series, etc.

</Param>
</ParamTable>

## Returns

An [`InputColumn`](/core/pydoc/code/deephaven.column.html#deephaven.column.InputColumn).

## Example

The following examples use [`new_table`](./newTable.md) to create a table with a single column of bytes named `Bytes`.

```python
from deephaven import new_table
from deephaven.column import byte_col

result = new_table([byte_col("Bytes", [1, 2, 3, 4])])
```

## Related documentation

- [Create a new table](../../../how-to-guides/new-and-empty-table.md#new_table)
- [`new_table`](./newTable.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/engine/util/TableTools.html#byteCol(java.lang.String,byte...))
- [Pydoc](/core/pydoc/code/deephaven.column.html#deephaven.column.byte_col)
