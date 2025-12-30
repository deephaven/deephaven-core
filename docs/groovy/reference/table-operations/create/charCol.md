---
title: charCol
---

The `charCol` method creates a column containing Java primitive character values.

> [!NOTE]
> This method is commonly used with [`newTable`](./newTable.md) to create tables.

## Syntax

```
charCol(name, data...)
```

## Parameters

<ParamTable>
<Param name="name" type="String">

The name of the new column.

</Param>
<Param name="data" type="char...">

The column values.

</Param>
</ParamTable>

## Returns

A [`ColumnHolder`](/core/javadoc/io/deephaven/engine/table/impl/util/ColumnHolder.html).

## Example

The following examples use [`newTable`](./newTable.md) to create a table with a single column of characters named `Chars`.

```groovy
result = newTable(
    charCol("Chars", 'a' as char, 'b' as char)
)
```

## Related documentation

- [Create a new table](../../../how-to-guides/new-and-empty-table.md#newtable)
- [`newTable`](./newTable.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/engine/util/TableTools.html#charCol(java.lang.String,char...))
