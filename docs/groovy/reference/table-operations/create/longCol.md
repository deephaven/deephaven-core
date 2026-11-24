---
title: longCol
---

The `longCol` method creates a column containing Java primitive `long` values (64-bit signed integers).

> [!NOTE]
> Pass the column that `longCol` returns to [`newTable`](./newTable.md) to create a table.

> [!IMPORTANT]
> The value -9223372036854775808 (`Long.MIN_VALUE`) is reserved to represent [null](../../query-language/types/nulls.md) in `long` columns. `long` columns do not support infinite or not-a-number (NaN) values.

## Syntax

```groovy syntax
longCol(name, data...)
```

## Parameters

<ParamTable>
<Param name="name" type="String">

The name of the new column.

</Param>
<Param name="data" type="long...">

The column values. Use [`NULL_LONG`](/core/javadoc/io/deephaven/util/QueryConstants.html#NULL_LONG) to store a null value.

</Param>
</ParamTable>

## Returns

A [`ColumnHolder`](/core/javadoc/io/deephaven/engine/table/impl/util/ColumnHolder.html).

## Example

The following example uses [`newTable`](./newTable.md) to create a table with a single column of longs named `Longs`.

```groovy
result = newTable(
    longCol("Longs", 10000000, 9876543210, -314159265)
)
```

## Related documentation

- [Create static tables](../../../how-to-guides/new-and-empty-table.md)
- [`newTable`](./newTable.md)
- [Nulls](../../query-language/types/nulls.md)
- [`TableTools.longCol` Javadoc](/core/javadoc/io/deephaven/engine/util/TableTools.html#longCol(java.lang.String,long...))
- [`ColumnHolder` Javadoc](/core/javadoc/io/deephaven/engine/table/impl/util/ColumnHolder.html)
- [`QueryConstants` Javadoc](/core/javadoc/io/deephaven/util/QueryConstants.html)
