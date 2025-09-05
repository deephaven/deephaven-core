---
title: longCol
---

The `longCol` method creates a column containing Java primitive long values.

> [!NOTE]
> This method is commonly used with [`newTable`](./newTable.md) to create tables.

## Syntax

```
longCol(name, data...)
```

## Parameters

<ParamTable>
<Param name="name" type="String">

The name of the new column.

</Param>
<Param name="data" type="long...">

The column values.

</Param>
</ParamTable>

## Returns

A [`ColumnHolder`](/core/javadoc/io/deephaven/engine/table/impl/util/ColumnHolder.html).

## Example

The following examples use [`newTable`](./newTable.md) to create a table with a single column of longs named `Longs`.

```groovy
result = newTable(
    longCol("Longs", 10000000, 987654321, -314159265)
)
```

## Related documentation

- [Create a new table](../../../how-to-guides/new-and-empty-table.md#newtable)
- [`newTable`](./newTable.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/engine/util/TableTools.html#longCol(java.lang.String,long...))
