---
title: double_col
---

The `double_col` method creates a column containing Java primitive double values.

> [!NOTE]
> This method is commonly used with [`new_table`](./newTable.md) to create tables.

## Syntax

```python syntax
double_col(name: str, data: Sequence[float]) -> InputColumn
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

The following examples use [`new_table`](./newTable.md) to create a table with a single column of doubles named `Doubles`.

```python
from deephaven import new_table
from deephaven.column import double_col

result = new_table([double_col("Doubles", [0.1, 0.2, 0.3])])
```

## Related documentation

- [Create a new table](../../../how-to-guides/new-and-empty-table.md#new_table)
- [`new_table`](./newTable.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/engine/util/TableTools.html#doubleCol(java.lang.String,double...))
- [Pydoc](/core/pydoc/code/deephaven.column.html#deephaven.column.double_col)
