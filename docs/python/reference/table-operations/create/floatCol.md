---
title: float_col
---

The `float_col` method creates a column containing Java primitive float values.

> [!NOTE]
> This method is commonly used with [`new_table`](./newTable.md) to create tables.

## Syntax

```python syntax
int_col(name: str, data: Sequence[float]) -> InputColumn
```

## Parameters

<ParamTable>
<Param name="name" type="str">

The name of the new column.

</Param>
<Param name="data" type="Sequence[float]">

The column values. This can be any sequence of compatible data, e.g., list, tuple, ndarray, pandas series, etc.

</Param>
</ParamTable>

## Returns

An [`InputColumn`](/core/pydoc/code/deephaven.column.html#deephaven.column.InputColumn).

## Example

The following examples use [`new_table`](./newTable.md) to create a table with a single column of floats named `Floats`.

```python
from deephaven import new_table
from deephaven.column import float_col

result = new_table([float_col("Floats", [9.9, 8.8, 7.7])])
```

## Related documentation

- [Create static tables](../../../how-to-guides/new-and-empty-table.md#new_table)
- [`new_table`](./newTable.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/engine/util/TableTools.html#floatCol(java.lang.String,float...))
- [Pydoc](/core/pydoc/code/deephaven.column.html#deephaven.column.float_col)
