---
title: doubleCol
---

The `doubleCol` method creates a column containing Java primitive `double` values (64-bit floating-point numbers).

> [!NOTE]
> Pass the column that `doubleCol` returns to [`newTable`](./newTable.md) to create a table.

> [!IMPORTANT]
> The value `-Double.MAX_VALUE` is reserved to represent [null](../../query-language/types/nulls.md) in `double` columns. [NaN](../../query-language/types/NaNs.md) and infinite values are stored as-is, not as null.

## Syntax

```groovy syntax
doubleCol(name, data...)
```

## Parameters

<ParamTable>
<Param name="name" type="String">

The name of the new column.

</Param>
<Param name="data" type="double...">

The column values. Use [`NULL_DOUBLE`](/core/javadoc/io/deephaven/util/QueryConstants.html#NULL_DOUBLE) to store a null value.

</Param>
</ParamTable>

## Returns

A [`ColumnHolder`](/core/javadoc/io/deephaven/engine/table/impl/util/ColumnHolder.html).

## Example

The following example uses [`newTable`](./newTable.md) to create a table with a single column of doubles named `Doubles`.

```groovy
result = newTable(
    doubleCol("Doubles", 0.1, 0.2, 0.3)
)
```

## Related documentation

- [Create static tables](../../../how-to-guides/new-and-empty-table.md)
- [`newTable`](./newTable.md)
- [NaNs](../../query-language/types/NaNs.md)
- [Nulls](../../query-language/types/nulls.md)
- [`TableTools.doubleCol` Javadoc](/core/javadoc/io/deephaven/engine/util/TableTools.html#doubleCol(java.lang.String,double...))
- [`ColumnHolder` Javadoc](/core/javadoc/io/deephaven/engine/table/impl/util/ColumnHolder.html)
- [`QueryConstants` Javadoc](/core/javadoc/io/deephaven/util/QueryConstants.html)
