---
title: charCol
---

The `charCol` method creates a column containing Java primitive character values.

> [!NOTE]
> Pass the column that `charCol` returns to [`newTable`](./newTable.md) to create a table.

> [!IMPORTANT]
> The value 65535 (`Character.MAX_VALUE`) is reserved to represent [null](../../query-language/types/nulls.md) in `char` columns.

## Syntax

```groovy syntax
charCol(name, data...)
```

## Parameters

<ParamTable>
<Param name="name" type="String">

The name of the new column.

</Param>
<Param name="data" type="char...">

The column values. Each value is a Java `char`, which holds one 16-bit UTF-16 code unit. Use [`NULL_CHAR`](/core/javadoc/io/deephaven/util/QueryConstants.html#NULL_CHAR) to store a [null](../../query-language/types/nulls.md) value.

</Param>
</ParamTable>

## Returns

A [`ColumnHolder`](/core/javadoc/io/deephaven/engine/table/impl/util/ColumnHolder.html).

## Example

The following example uses [`newTable`](./newTable.md) to create a table with a single column of characters named `Chars` that includes a null value. A single-quoted Groovy literal such as `'a'` is a `String`, so each literal is cast with `as char`. Casting a string of more than one character with `as char` keeps only the first character and does not raise an error: `"ab" as char` is `a`.

```groovy
result = newTable(
    charCol("Chars", 'a' as char, 'b' as char, NULL_CHAR)
)
```

## Related documentation

- [Create static tables](../../../how-to-guides/new-and-empty-table.md)
- [`newTable`](./newTable.md)
- [Nulls](../../query-language/types/nulls.md)
- [`TableTools.charCol` Javadoc](/core/javadoc/io/deephaven/engine/util/TableTools.html#charCol(java.lang.String,char...))
- [`ColumnHolder` Javadoc](/core/javadoc/io/deephaven/engine/table/impl/util/ColumnHolder.html)
- [`QueryConstants` Javadoc](/core/javadoc/io/deephaven/util/QueryConstants.html)
