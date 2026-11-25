---
title: intCol
---

The `intCol` method creates a column containing Java primitive `int` values (32-bit signed integers).

> [!NOTE]
> Pass the column that `intCol` returns to [`newTable`](./newTable.md) to create a table.

> [!IMPORTANT]
> The value -2147483648 (`Integer.MIN_VALUE`) is reserved to represent [null](../../query-language/types/nulls.md) in `int` columns. A cast that overflows, such as `(int)2147483648L`, wraps around to -2147483648 and so becomes null. `int` columns do not support infinite or not-a-number (NaN) values.

## Syntax

```groovy syntax
intCol(name, data...)
```

## Parameters

<ParamTable>
<Param name="name" type="String">

The name of the new column.

</Param>
<Param name="data" type="int...">

The column values. Use [`NULL_INT`](/core/javadoc/io/deephaven/util/QueryConstants.html#NULL_INT) to store a null value.

</Param>
</ParamTable>

## Returns

A [`ColumnHolder`](/core/javadoc/io/deephaven/engine/table/impl/util/ColumnHolder.html).

## Example

The following example uses [`newTable`](./newTable.md) to create a table with a single column of integers named `Integers`.

```groovy
result = newTable(
    intCol("Integers", 1, 2, 3, 4, 5)
)
```

## Related documentation

- [Create static tables](../../../how-to-guides/new-and-empty-table.md)
- [`newTable`](./newTable.md)
- [Nulls](../../query-language/types/nulls.md)
- [`TableTools.intCol` Javadoc](/core/javadoc/io/deephaven/engine/util/TableTools.html#intCol(java.lang.String,int...))
- [`ColumnHolder` Javadoc](/core/javadoc/io/deephaven/engine/table/impl/util/ColumnHolder.html)
- [`QueryConstants` Javadoc](/core/javadoc/io/deephaven/util/QueryConstants.html)
