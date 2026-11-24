---
title: char_col
---

The `char_col` function creates a column containing Java primitive character values.

> [!NOTE]
> Pass the column that `char_col` returns to [`new_table`](./newTable.md) to create a table.

> [!IMPORTANT]
> The value 65535 (`Character.MAX_VALUE`, available as [`NULL_CHAR`](/core/pydoc/code/deephaven.constants.html#deephaven.constants.NULL_CHAR)) is reserved to represent [null](../../query-language/types/nulls.md) in `char` columns.

## Syntax

```python syntax
char_col(name: str, data: Sequence) -> InputColumn
```

## Parameters

<ParamTable>
<Param name="name" type="str">

The name of the new column.

</Param>
<Param name="data" type="Sequence">

The column values. This can be a string, in which case each character becomes one value. It can also be a sequence of integer character codes from 0 to 65534, such as a list, tuple, NumPy array, or pandas Series. In a sequence of codes, `None` values become null.

Each value is a Java `char`, which holds one 16-bit UTF-16 code unit. Characters above U+FFFF, such as most emoji, do not fit in one value.

</Param>
</ParamTable>

## Returns

An [`InputColumn`](./inputColumn.md).

## Example

The following example uses [`new_table`](./newTable.md) to create a table with two character columns. `Chars` comes from a string, and `FromCodes` comes from a list of character codes that includes a null value.

```python
from deephaven import new_table
from deephaven.column import char_col

result = new_table([char_col("Chars", "abc"), char_col("FromCodes", [97, 98, None])])
```

## Related documentation

- [Create static tables](../../../how-to-guides/new-and-empty-table.md)
- [`new_table`](./newTable.md)
- [`InputColumn`](./inputColumn.md)
- [Nulls](../../query-language/types/nulls.md)
- [`deephaven.constants` Pydoc](/core/pydoc/code/deephaven.constants.html)
- [Pydoc](/core/pydoc/code/deephaven.column.html#deephaven.column.char_col)
