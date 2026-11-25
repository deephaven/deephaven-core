---
title: byteCol
---

The `byteCol` method creates a column containing Java primitive `byte` values (8-bit signed integers).

> [!NOTE]
> Pass the column that `byteCol` returns to [`newTable`](./newTable.md) to create a table.

> [!IMPORTANT]
> The value -128 (`Byte.MIN_VALUE`) is reserved to represent [null](../../query-language/types/nulls.md) in `byte` columns. A cast that overflows, such as `(byte)128`, wraps around to -128 and so becomes null.

## Syntax

```groovy syntax
byteCol(name, data...)
```

## Parameters

<ParamTable>
<Param name="name" type="String">

The name of the new column.

</Param>
<Param name="data" type="byte...">

The column values. Each value must be a `byte`. Groovy integer literals are `Integer` values, so cast each literal, for example `(byte)1`. Use [`NULL_BYTE`](/core/javadoc/io/deephaven/util/QueryConstants.html#NULL_BYTE) to store a null value.

</Param>
</ParamTable>

## Returns

A [`ColumnHolder`](/core/javadoc/io/deephaven/engine/table/impl/util/ColumnHolder.html).

## Example

The following example uses [`newTable`](./newTable.md) to create a table with a single column of bytes named `Bytes`.

```groovy
result = newTable(
    byteCol("Bytes", (byte)1, (byte)2, (byte)3, (byte)4)
)
```

## Related documentation

- [Create static tables](../../../how-to-guides/new-and-empty-table.md)
- [`newTable`](./newTable.md)
- [Nulls](../../query-language/types/nulls.md)
- [`TableTools.byteCol` Javadoc](/core/javadoc/io/deephaven/engine/util/TableTools.html#byteCol(java.lang.String,byte...))
- [`ColumnHolder` Javadoc](/core/javadoc/io/deephaven/engine/table/impl/util/ColumnHolder.html)
- [`QueryConstants` Javadoc](/core/javadoc/io/deephaven/util/QueryConstants.html)
