---
title: newTable
---

The `newTable` method creates an in-memory table from a list of columns. Each column must have an equal number of elements.

## Syntax

```
newTable(size, names, columnSources)
newTable(size, columns)
newTable(definition)
newTable(columnHolders...)
newTable(definition, columnHolders...)
```

## Parameters

<ParamTable>
<Param name="columnHolders" type="ColumnHolder...">

A list of columnHolders from which to create the new table.

Columns are created using the following methods:

- [`boolCol`](./booleanCol.md)
- [`byteCol`](./byteCol.md)
- [`charCol`](./charCol.md)
- [`col`](./col.md)
- [`doubleCol`](./doubleCol.md)
- [`floatCol`](./floatCol.md)
- [`instantCol`](./instantCol.md)
- [`intCol`](./intCol.md)
- [`longCol`](./longCol.md)
- [`shortCol`](./shortCol.md)
- [`stringCol`](./stringCol.md)

</Param>
<Param name="names" type="String...">

A list of column names.

</Param>
<Param name="size" type="long">

The number of rows to allocate.

</Param>
<Param name="columns" type="Map">

A Map of column names and ColumnSources.

<!--TODO: add an example of this format?-->
</Param>
<Param name="definition" type="TableDefinition">

The TableDefinition (column names and properties) to use for the new table.

</Param>
</ParamTable>

## Returns

An in-memory table.

## Examples

The following example creates a table with one column of three integer values.

```groovy
result = newTable(
    intCol("IntegerColumn", 1, 2, 3)
)
```

The following example creates a table with a double and a string column.

```groovy
result = newTable(
    doubleCol("Doubles", 3.1, 5.45, -1.0),
    stringCol("Strings", "Creating", "New", "Tables")
)
```

<!--TODO: add more code examples for some of the other overloads-->

## Related documentation

- [Create new and empty tables](../../../how-to-guides/new-and-empty-table.md)
- [`emptyTable`](./emptyTable.md)
- [`boolCol`](./booleanCol.md)
- [`byteCol`](./byteCol.md)
- [`charCol`](./charCol.md)
- [`col`](./col.md)
- [`instantCol`](./instantCol.md)
- [`doubleCol`](./doubleCol.md)
- [`floatCol`](./floatCol.md)
- [`longCol`](./longCol.md)
- [`shortCol`](./shortCol.md)
- [`stringCol`](./stringCol.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/engine/util/TableTools.html#emptyTable(long))
