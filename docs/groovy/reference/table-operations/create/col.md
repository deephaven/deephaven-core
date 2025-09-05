---
title: col
---

The `col` method creates a column containing Java objects.

> [!NOTE]
> This method is commonly used with [`newTable`](./newTable.md) to create tables.

> [!CAUTION]
> `col` is significantly slower than other methods that create columns. When creating a column with data of one type, use the corresponding specialized method (e.g. for ints, use [`intCol`](./intCol.md)).

## Syntax

```
col(name, data...)
```

## Parameters

<ParamTable>
<Param name="name" type="String">

The name of the new column.

</Param>
<Param name="data" type="Java.lang.Object...">

The column values.

</Param>
</ParamTable>

## Returns

A [`ColumnHolder`](/core/javadoc/io/deephaven/engine/table/impl/util/ColumnHolder.html).

## Example

The following examples use [`newTable`](./newTable.md) to create a table with a single column of objects named `Values`.

```groovy
result = newTable(
    col("Values", "a", 1, -5.5)
)
```

## Related documentation

- [Create a new table](../../../how-to-guides/new-and-empty-table.md#newtable)
- [`newTable`](./newTable.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/engine/util/TableTools.html#col(java.lang.String,T...))
