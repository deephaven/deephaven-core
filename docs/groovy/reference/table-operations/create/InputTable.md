---
title: Input table
---

In-memory input tables are Deephaven tables that users can change manually in the UI or programmatically through an [`InputTableUpdater`](/core/javadoc/io/deephaven/engine/util/input/InputTableUpdater.html). An in-memory input table is either append-only or keyed:

- [`AppendOnlyArrayBackedInputTable`](/core/javadoc/io/deephaven/engine/table/impl/util/AppendOnlyArrayBackedInputTable.html): an append-only input table adds new rows to the end of the table and does not support edits or deletions.
- [`KeyedArrayBackedInputTable`](/core/javadoc/io/deephaven/engine/table/impl/util/KeyedArrayBackedInputTable.html): a keyed input table has one or more key columns. It adds, modifies, and deletes rows by key, and an added row whose key matches an existing row replaces that row.

In the UI, added rows take effect when the user clicks **Commit**, and edits to existing rows of a keyed input table take effect immediately. See [How to use input tables](../../../how-to-guides/input-tables.md) for details, including [how to add data in the UI](../../../how-to-guides/input-tables.md#add-data-to-the-table).

## Syntax

### Append-only

```groovy syntax
AppendOnlyArrayBackedInputTable.make(definition)
AppendOnlyArrayBackedInputTable.make(initialTable)
```

### Keyed

```groovy syntax
KeyedArrayBackedInputTable.make(definition, keyColumnNames...)
KeyedArrayBackedInputTable.make(initialTable, keyColumnNames...)
```

## Parameters

<ParamTable>
<Param name="definition" type="TableDefinition">

The [`TableDefinition`](/core/javadoc/io/deephaven/engine/table/TableDefinition.html) of the new table. The resulting input table is initially empty.

</Param>
<Param name="initialTable" type="Table">

The initial [`Table`](/core/javadoc/io/deephaven/engine/table/Table.html) to copy into the append-only or keyed input table. The input table has the same column definitions as `initialTable`. In a keyed input table, if several rows of `initialTable` share a key, only the last of those rows is kept.

</Param>
<Param name="keyColumnNames" type="String...">

The names of the key columns. This parameter applies only to keyed input tables. If a name is not a column of `definition` or `initialTable`, `make` throws an exception.

</Param>
</ParamTable>

## Returns

An [`AppendOnlyArrayBackedInputTable`](/core/javadoc/io/deephaven/engine/table/impl/util/AppendOnlyArrayBackedInputTable.html) from `AppendOnlyArrayBackedInputTable.make`, or a [`KeyedArrayBackedInputTable`](/core/javadoc/io/deephaven/engine/table/impl/util/KeyedArrayBackedInputTable.html) from `KeyedArrayBackedInputTable.make`. Both implement [`Table`](/core/javadoc/io/deephaven/engine/table/Table.html).

## Methods

To add or delete data programmatically, get the input table's [`InputTableUpdater`](/core/javadoc/io/deephaven/engine/util/input/InputTableUpdater.html) with [`InputTableUpdater.from(table)`](/core/javadoc/io/deephaven/engine/util/input/InputTableUpdater.html#from(io.deephaven.engine.table.Table)). An `InputTableUpdater` supports the following methods, among others:

- [`add`](/core/javadoc/io/deephaven/engine/util/input/InputTableUpdater.html#add(io.deephaven.engine.table.Table)): add data to an input table. The added data must fit the schema of the input table.
- [`addAsync`](/core/javadoc/io/deephaven/engine/util/input/InputTableUpdater.html#addAsync(io.deephaven.engine.table.Table,io.deephaven.engine.util.input.InputTableStatusListener)): add data to an input table asynchronously. The added data must fit the schema of the input table. Deephaven notifies the given [`InputTableStatusListener`](/core/javadoc/io/deephaven/engine/util/input/InputTableStatusListener.html) when the addition succeeds or fails.
- [`delete`](/core/javadoc/io/deephaven/engine/util/input/InputTableUpdater.html#delete(io.deephaven.engine.table.Table)): delete the rows whose keys are in the given table, which must contain all of the input table's key columns and no other columns. Only keyed input tables support this method.
- [`deleteAsync`](/core/javadoc/io/deephaven/engine/util/input/InputTableUpdater.html#deleteAsync(io.deephaven.engine.table.Table,io.deephaven.engine.util.input.InputTableStatusListener)): asynchronously delete the rows whose keys are in the given table, which must contain all of the input table's key columns and no other columns. Only keyed input tables support this method. Deephaven notifies the given [`InputTableStatusListener`](/core/javadoc/io/deephaven/engine/util/input/InputTableStatusListener.html) when the deletion succeeds or fails.
- [`getKeyNames`](/core/javadoc/io/deephaven/engine/util/input/InputTableUpdater.html#getKeyNames()): get the names of the key columns.
- [`getValueNames`](/core/javadoc/io/deephaven/engine/util/input/InputTableUpdater.html#getValueNames()): get the names of the value columns. By default, every column that is not a key column is a value column.

## Examples

The following example creates an append-only input table from a source table.

```groovy order=source,result
import io.deephaven.engine.table.impl.util.AppendOnlyArrayBackedInputTable

source = newTable(
    doubleCol("Doubles", 3.1, 5.45, -1.0),
    stringCol("Strings", "Creating", "New", "Tables")
)

result = AppendOnlyArrayBackedInputTable.make(source)
```

The following example creates an append-only input table from a [`TableDefinition`](/core/javadoc/io/deephaven/engine/table/TableDefinition.html). The table is initially empty.

```groovy order=myInputTable
import io.deephaven.engine.table.impl.util.AppendOnlyArrayBackedInputTable
import io.deephaven.engine.table.ColumnDefinition
import io.deephaven.engine.table.TableDefinition

myColumnDefs = TableDefinition.of(
    ColumnDefinition.ofString("StringCol"),
    ColumnDefinition.ofDouble("DoubleCol")
)

myInputTable = AppendOnlyArrayBackedInputTable.make(myColumnDefs)
```

The following example creates a keyed input table from a source table, with `Strings` as the key column.

```groovy order=source,result
import io.deephaven.engine.table.impl.util.KeyedArrayBackedInputTable

source = newTable(
    doubleCol("Doubles", 3.1, 5.45, -1.0),
    stringCol("Strings", "Creating", "New", "Tables")
)

result = KeyedArrayBackedInputTable.make(source, "Strings")
```

The following example adds data to an input table with the [`add`](/core/javadoc/io/deephaven/engine/util/input/InputTableUpdater.html#add(io.deephaven.engine.table.Table)) method of its [`InputTableUpdater`](/core/javadoc/io/deephaven/engine/util/input/InputTableUpdater.html).

```groovy order=myTable,myInputTable
import io.deephaven.engine.table.impl.util.AppendOnlyArrayBackedInputTable
import io.deephaven.engine.table.ColumnDefinition
import io.deephaven.engine.table.TableDefinition
import io.deephaven.engine.util.input.InputTableUpdater

myColDefs = TableDefinition.of(ColumnDefinition.ofInt("X"), ColumnDefinition.ofDouble("Y"))
myTable = emptyTable(5).update("X = i", "Y = (double)(2 * i)")
myInputTable = AppendOnlyArrayBackedInputTable.make(myColDefs)
InputTableUpdater.from(myInputTable).add(myTable)
```

## Related documentation

- [`emptyTable`](./emptyTable.md)
- [`newTable`](./newTable.md)
- [Create static tables](../../../how-to-guides/new-and-empty-table.md)
- [How to use input tables](../../../how-to-guides/input-tables.md)
- [Table types](../../../conceptual/table-types.md)
- [AppendOnlyArrayBackedInputTable Javadoc](/core/javadoc/io/deephaven/engine/table/impl/util/AppendOnlyArrayBackedInputTable.html)
- [KeyedArrayBackedInputTable Javadoc](/core/javadoc/io/deephaven/engine/table/impl/util/KeyedArrayBackedInputTable.html)
- [InputTableUpdater Javadoc](/core/javadoc/io/deephaven/engine/util/input/InputTableUpdater.html)
- [InputTableStatusListener Javadoc](/core/javadoc/io/deephaven/engine/util/input/InputTableStatusListener.html)
- [TableDefinition Javadoc](/core/javadoc/io/deephaven/engine/table/TableDefinition.html)
- [Table Javadoc](/core/javadoc/io/deephaven/engine/table/Table.html)
