---
title: stringCol
---

The `stringCol` method creates a column containing Java `String` values.

> [!NOTE]
> Pass the column that `stringCol` returns to [`newTable`](./newTable.md) to create a table.

## Syntax

```groovy syntax
stringCol(name, data...)
```

## Parameters

<ParamTable>
<Param name="name" type="String">

The name of the new column.

</Param>
<Param name="data" type="String...">

The column values. Use `null` to store a [null](../../query-language/types/nulls.md) value.

</Param>
</ParamTable>

## Returns

A [`ColumnHolder`](/core/javadoc/io/deephaven/engine/table/impl/util/ColumnHolder.html).

## Example

The following example uses [`newTable`](./newTable.md) to create a table with a single column of strings named `Strings`. `stringCol` does not parse the values, so `"3.14"` stays a string and is not converted to a number.

```groovy
result = newTable(
    stringCol("Strings", "Deephaven", "3.14", "Community")
)
```

## Related documentation

- [Create static tables](../../../how-to-guides/new-and-empty-table.md)
- [`newTable`](./newTable.md)
- [Nulls](../../query-language/types/nulls.md)
- [`TableTools.stringCol` Javadoc](/core/javadoc/io/deephaven/engine/util/TableTools.html#stringCol(java.lang.String,java.lang.String...))
- [`ColumnHolder` Javadoc](/core/javadoc/io/deephaven/engine/table/impl/util/ColumnHolder.html)
