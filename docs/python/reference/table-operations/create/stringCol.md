---
title: string_col
---

The `string_col` function creates a column containing Java `String` values.

> [!NOTE]
> Pass the column that `string_col` returns to [`new_table`](./newTable.md) to create a table.

## Syntax

```python syntax
string_col(name: str, data: Sequence) -> InputColumn
```

## Parameters

<ParamTable>
<Param name="name" type="str">

The name of the new column.

</Param>
<Param name="data" type="Sequence">

The column values, as `str` values, such as a list, tuple, NumPy array, or pandas Series. `None` values become [null](../../query-language/types/nulls.md).

</Param>
</ParamTable>

## Returns

An [`InputColumn`](./inputColumn.md).

## Example

The following example uses [`new_table`](./newTable.md) to create a table with a single column of strings named `Strings`. `string_col` does not parse the values, so `"3.14"` stays a string and is not converted to a number.

```python
from deephaven import new_table
from deephaven.column import string_col

result = new_table([string_col("Strings", ["Deephaven", "3.14", "Community"])])
```

## Related documentation

- [Create static tables](../../../how-to-guides/new-and-empty-table.md)
- [`new_table`](./newTable.md)
- [`InputColumn`](./inputColumn.md)
- [Nulls](../../query-language/types/nulls.md)
- [Pydoc](/core/pydoc/code/deephaven.column.html#deephaven.column.string_col)
