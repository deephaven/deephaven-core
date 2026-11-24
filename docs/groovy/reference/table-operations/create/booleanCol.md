---
title: booleanCol
---

The `booleanCol` method creates a column containing Java `Boolean` values.

> [!NOTE]
> Pass the column that `booleanCol` returns to [`newTable`](./newTable.md) to create a table.

## Syntax

```groovy syntax
booleanCol(name, data...)
```

## Parameters

<ParamTable>
<Param name="name" type="String">

The name of the new column.

</Param>
<Param name="data" type="Boolean...">

The column values. Use `null` to store a [null](../../query-language/types/nulls.md) value.

</Param>
</ParamTable>

## Returns

A [`ColumnHolder`](/core/javadoc/io/deephaven/engine/table/impl/util/ColumnHolder.html).

## Example

The following example uses [`newTable`](./newTable.md) to create a table with a single column of boolean values named `Booleans`.

```groovy
result = newTable(
    booleanCol("Booleans", true, false, false, true)
)
```

## Related documentation

- [Create static tables](../../../how-to-guides/new-and-empty-table.md)
- [`newTable`](./newTable.md)
- [Nulls](../../query-language/types/nulls.md)
- [`TableTools.booleanCol` Javadoc](/core/javadoc/io/deephaven/engine/util/TableTools.html#booleanCol(java.lang.String,java.lang.Boolean...))
- [`ColumnHolder` Javadoc](/core/javadoc/io/deephaven/engine/table/impl/util/ColumnHolder.html)
