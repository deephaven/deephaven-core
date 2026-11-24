---
title: newTable
---

The `newTable` method creates a table from one or more [`ColumnHolder`](/core/javadoc/io/deephaven/engine/table/impl/util/ColumnHolder.html) objects, from existing [`ColumnSource`](/core/javadoc/io/deephaven/engine/table/ColumnSource.html)s, or from a [`TableDefinition`](/core/javadoc/io/deephaven/engine/table/TableDefinition.html) alone. When `newTable` builds the table from [`ColumnHolder`](/core/javadoc/io/deephaven/engine/table/impl/util/ColumnHolder.html)s, each column must have the same number of elements.

## Syntax

```groovy syntax
newTable(columnHolders...)
newTable(definition)
newTable(definition, columnHolders...)
newTable(definition, attributes, columnHolders...)
newTable(size, names, columnSources)
newTable(size, columns)
```

## Parameters

<ParamTable>
<Param name="columnHolders" type="ColumnHolder...">

One or more [`ColumnHolder`](/core/javadoc/io/deephaven/engine/table/impl/util/ColumnHolder.html) objects, each holding a column name and its data. The new table gets one column per holder. If the call also includes a `definition`, that definition must match these holders, as described under `definition`.

Each of the following methods returns a [`ColumnHolder`](/core/javadoc/io/deephaven/engine/table/impl/util/ColumnHolder.html):

- [`booleanCol`](./booleanCol.md)
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
<Param name="definition" type="TableDefinition">

The [`TableDefinition`](/core/javadoc/io/deephaven/engine/table/TableDefinition.html) (column names and properties) to use for the new table. With `columnHolders`, the definition must describe the same columns as the holders, with the same names, types, and order. `newTable` does not check that they match, so a mismatched definition produces a table whose schema disagrees with its data.

</Param>
<Param name="attributes" type="Map<String, Object>">

[Table attributes](../metadata/getAttributes.md) to set on the new table. This parameter can be `null`.

</Param>
<Param name="size" type="long">

The number of rows in the new table. The new table reads each [`ColumnSource`](/core/javadoc/io/deephaven/engine/table/ColumnSource.html) at row keys 0 through `size - 1`, so each source must hold unchanging data at those row keys. The [`ColumnSource`](/core/javadoc/io/deephaven/engine/table/ColumnSource.html)s of a [static](../../../conceptual/table-types.md#static-tables), [flat](./flatten.md) table meet this requirement, because that table's row keys run from 0 through `size - 1` with no gaps. To copy rows from a filtered or [refreshing](../../../conceptual/table-types.md#standard-streaming-tables) table, use [`select`](../select/select.md) or [`snapshot`](../snapshot/snapshot.md) instead.

</Param>
<Param name="names" type="List<String>">

A list of column names.

</Param>
<Param name="columnSources" type="List<ColumnSource<?>>">

A list of the [`ColumnSource`](/core/javadoc/io/deephaven/engine/table/ColumnSource.html)s that supply the column data, in the same order as `names`.

</Param>
<Param name="columns" type="Map<String, ColumnSource<?>>">

A map from each column name to the [`ColumnSource`](/core/javadoc/io/deephaven/engine/table/ColumnSource.html) that supplies that column's data.

</Param>
</ParamTable>

## Returns

A new table. The `columnHolders` overloads hold their data in memory. `newTable(definition)` returns an empty table with the definition's columns.

The `size` overloads return a [static table](../../../conceptual/table-types.md#static-tables) that wraps the supplied [`ColumnSource`](/core/javadoc/io/deephaven/engine/table/ColumnSource.html)s without copying them.

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

The following example creates an empty table with an integer and a string column from a [`TableDefinition`](/core/javadoc/io/deephaven/engine/table/TableDefinition.html).

```groovy
import io.deephaven.engine.table.ColumnDefinition
import io.deephaven.engine.table.TableDefinition

definition = TableDefinition.of(
    ColumnDefinition.ofInt("IntegerColumn"),
    ColumnDefinition.ofString("StringColumn")
)

result = newTable(definition)
```

The following example builds a new table from the [`ColumnSource`](/core/javadoc/io/deephaven/engine/table/ColumnSource.html)s of an existing [static](../../../conceptual/table-types.md#static-tables), [flat](./flatten.md) table. It passes `source.size()` as `size`, and gives the column names and sources first as two lists in the same order, and then as a map.

```groovy order=resultFromLists,resultFromMap,source
source = emptyTable(3).update("X = i", "Y = X * 1.5")

resultFromLists = newTable(
    source.size(),
    ["X", "Y"],
    [source.getColumnSource("X"), source.getColumnSource("Y")]
)

resultFromMap = newTable(
    source.size(),
    [X: source.getColumnSource("X"), Y: source.getColumnSource("Y")]
)
```

## Related documentation

- [Create static tables](../../../how-to-guides/new-and-empty-table.md)
- [`emptyTable`](./emptyTable.md)
- [`flatten`](./flatten.md)
- [`select`](../select/select.md)
- [`snapshot`](../snapshot/snapshot.md)
- [`getAttributes`](../metadata/getAttributes.md)
- [Table types](../../../conceptual/table-types.md)
- [`ColumnHolder`](/core/javadoc/io/deephaven/engine/table/impl/util/ColumnHolder.html)
- [`ColumnSource`](/core/javadoc/io/deephaven/engine/table/ColumnSource.html)
- [`TableDefinition`](/core/javadoc/io/deephaven/engine/table/TableDefinition.html)
- [`booleanCol`](./booleanCol.md)
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
- [Javadoc](/core/javadoc/io/deephaven/engine/util/TableTools.html#newTable(io.deephaven.engine.table.impl.util.ColumnHolder...))
