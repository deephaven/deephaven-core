---
title: char_col
---

The `char_col` method creates a column containing Java primitive character values.

> [!NOTE]
> This method is commonly used with [`new_table`](./newTable.md) to create tables.

## Syntax

```python syntax
char_col(name: str, data: Sequence[char]) -> InputColumn
```

## Parameters

<ParamTable>
<Param name="name" type="str">

The name of the new column.

</Param>
<Param name="data" type="Sequence[char]">

The column values. This can be any sequence of compatible data, e.g., list, tuple, ndarray, pandas series, etc.

</Param>
</ParamTable>

## Returns

An [`InputColumn`](/core/pydoc/code/deephaven.column.html#deephaven.column.InputColumn).

## Example

The following examples use [`new_table`](./newTable.md) to create a table with a single column of characters named `Chars`.

```python order=result
from deephaven import new_table
from deephaven.column import char_col

result = new_table([char_col("Chars", "ab")])
```

## Related documentation

- [Create a new table](../../../how-to-guides/new-and-empty-table.md#new_table)
- [`new_table`](./newTable.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/engine/util/TableTools.html#charCol(java.lang.String,char...))
- [Pydoc](/core/pydoc/code/deephaven.column.html#deephaven.column.char_col)
