---
title: byteCol
---

The `byteCol` method creates a column containing Java primitive byte values.

> [!NOTE]
> This method is commonly used with [`newTable`](./newTable.md) to create tables.

## Syntax

```
byteCol(name, data...)
```

## Parameters

<ParamTable>
<Param name="name" type="String">

The name of the new column.

</Param>
<Param name="data" type="byte...">

The column values.

</Param>
</ParamTable>

## Returns

A [`ColumnHolder`](/core/javadoc/io/deephaven/engine/table/impl/util/ColumnHolder.html).

## Example

The following examples use [`newTable`](./newTable.md) to create a table with a single column of bytes named `Bytes`.

```groovy
result = newTable(
    byteCol("Bytes", (byte)1, (byte)2, (byte)3, (byte)4)
)
```

## Related documentation

- [Create a new table](../../../how-to-guides/new-and-empty-table.md#newtable)
- [`newTable`](./newTable.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/engine/util/TableTools.html#byteCol(java.lang.String,byte...))
