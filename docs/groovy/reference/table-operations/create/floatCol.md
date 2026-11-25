---
title: floatCol
---

The `floatCol` method creates a column containing Java primitive `float` values (32-bit floating-point numbers).

> [!NOTE]
> Pass the column that `floatCol` returns to [`newTable`](./newTable.md) to create a table.

> [!IMPORTANT]
> The value `-Float.MAX_VALUE` is reserved to represent [null](../../query-language/types/nulls.md) in `float` columns. [NaN](../../query-language/types/NaNs.md) and infinite values are stored as-is, not as null.

## Syntax

```groovy syntax
floatCol(name, data...)
```

## Parameters

<ParamTable>
<Param name="name" type="String">

The name of the new column.

</Param>
<Param name="data" type="float...">

The column values. Each value must be a `float`. Groovy treats a decimal literal such as `9.9` as a `BigDecimal`, so cast it with `(float)` or write it with an `f` suffix, such as `9.9f`. Use [`NULL_FLOAT`](/core/javadoc/io/deephaven/util/QueryConstants.html#NULL_FLOAT) to store a null value.

</Param>
</ParamTable>

## Returns

A [`ColumnHolder`](/core/javadoc/io/deephaven/engine/table/impl/util/ColumnHolder.html).

## Example

The following example uses [`newTable`](./newTable.md) to create a table with a single column of floats named `Floats`.

```groovy
result = newTable(
    floatCol("Floats", (float)9.9, (float)8.8, (float)7.7)
)
```

## Related documentation

- [Create static tables](../../../how-to-guides/new-and-empty-table.md)
- [`newTable`](./newTable.md)
- [NaNs](../../query-language/types/NaNs.md)
- [Nulls](../../query-language/types/nulls.md)
- [`TableTools.floatCol` Javadoc](/core/javadoc/io/deephaven/engine/util/TableTools.html#floatCol(java.lang.String,float...))
- [`ColumnHolder` Javadoc](/core/javadoc/io/deephaven/engine/table/impl/util/ColumnHolder.html)
- [`QueryConstants` Javadoc](/core/javadoc/io/deephaven/util/QueryConstants.html)
