---
title: shortCol
---

The `shortCol` method creates a column containing Java primitive `short` values (16-bit signed integers).

> [!NOTE]
> Pass the column that `shortCol` returns to [`newTable`](./newTable.md) to create a table.

> [!IMPORTANT]
> The value -32768 (`Short.MIN_VALUE`) is reserved to represent [null](../../query-language/types/nulls.md) in `short` columns. A cast that overflows, such as `(short)32768`, wraps around to -32768 and so becomes null.

## Syntax

```groovy syntax
shortCol(name, data...)
```

## Parameters

<ParamTable>
<Param name="name" type="String">

The name of the new column.

</Param>
<Param name="data" type="short...">

The column values. Each value must be a `short`. Groovy integer literals are `Integer` values, so cast each literal, for example `(short)86`. Use [`NULL_SHORT`](/core/javadoc/io/deephaven/util/QueryConstants.html#NULL_SHORT) to store a null value.

</Param>
</ParamTable>

## Returns

A [`ColumnHolder`](/core/javadoc/io/deephaven/engine/table/impl/util/ColumnHolder.html).

## Example

The following example uses [`newTable`](./newTable.md) to create a table with a single column of shorts named `Shorts`.

```groovy
result = newTable(
    shortCol("Shorts", (short)86, (short)78, (short)41, (short)54, (short)20)
)
```

## Related documentation

- [Create static tables](../../../how-to-guides/new-and-empty-table.md)
- [`newTable`](./newTable.md)
- [Nulls](../../query-language/types/nulls.md)
- [`TableTools.shortCol` Javadoc](/core/javadoc/io/deephaven/engine/util/TableTools.html#shortCol(java.lang.String,short...))
- [`ColumnHolder` Javadoc](/core/javadoc/io/deephaven/engine/table/impl/util/ColumnHolder.html)
- [`QueryConstants` Javadoc](/core/javadoc/io/deephaven/util/QueryConstants.html)
