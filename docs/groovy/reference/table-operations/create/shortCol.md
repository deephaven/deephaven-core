---
title: shortCol
---

The `shortCol` method creates a column containing Java primitive short values.

> [!NOTE]
> This method is commonly used with [`newTable`](./newTable.md) to create tables.

## Syntax

```
shortCol(name, data...)
```

## Parameters

<ParamTable>
<Param name="name" type="String">

The name of the new column.

</Param>
<Param name="data" type="short...">

The column values.

</Param>
</ParamTable>

## Returns

A [`ColumnHolder`](/core/javadoc/io/deephaven/engine/table/impl/util/ColumnHolder.html).

## Example

The following examples use [`newTable`](./newTable.md) to create a table with a single column of shorts named `Shorts`.

```groovy
result = newTable(
    shortCol("Shorts", (short)86, (short)78, (short)41, (short)54, (short)20)
)
```

## Related documentation

- [How to create static tables](../../../how-to-guides/new-and-empty-table.md)
- [`newTable`](./newTable.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/engine/util/TableTools.html#shortCol(java.lang.String,short...))
