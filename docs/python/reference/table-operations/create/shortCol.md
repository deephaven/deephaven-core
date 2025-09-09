---
title: short_col
---

The `short_col` method creates a column containing Java primitive short values.

> [!NOTE]
> This method is commonly used with [`new_table`](./newTable.md) to create tables.

## Syntax

```python syntax
short_col(name: str, data: Sequence[int]) -> InputColumn
```

## Parameters

<ParamTable>
<Param name="name" type="str">

The name of the new column.

</Param>
<Param name="data" type="Sequence[int]">

The column values. This can be any sequence of compatible data, e.g., list, tuple, ndarray, pandas series, etc.

</Param>
</ParamTable>

## Returns

An [`InputColumn`](/core/pydoc/code/deephaven.column.html#deephaven.column.InputColumn).

## Example

The following examples use [`new_table`](./newTable.md) to create a table with a single column of shorts named `Shorts`.

```python
from deephaven import new_table
from deephaven.column import short_col

result = new_table([short_col("Shorts", [86, 78, 41, 54, 20])])
```

## Related documentation

- [How to create static tables](../../../how-to-guides/new-and-empty-table.md)
- [`new_table`](./newTable.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/engine/util/TableTools.html#shortCol(java.lang.String,short...))
- [Pydoc](/core/pydoc/code/deephaven.column.html#deephaven.column.short_col)
