---
title: tree
---

The Deephaven `tree` method creates a tree table from a source table given an ID column name and parent column name.

## Syntax

```
resultNoOrphans = source.tree(idColumn, parentColumn)
resultWithOrphans = TreeTable.promoteOrphans(source, idColumn, parentColumn).tree(idColumn, parentColumn)
```

## Parameters

<ParamTable>
<Param name="idColumn>" type="String">

The name of the ID column.

</Param>
<Param name="parentColumn" type="String">

The name of the parent column.

</Param>
</ParamTable>

## Methods

### Static

The `TreeTable` interface has the following static methods:

- [`promoteOrphans(Table, String, String)`](https://deephaven.io/core/javadoc/io/deephaven/engine/table/hierarchical/TreeTable.html#promoteOrphans(io.deephaven.engine.table.Table,java.lang.String,java.lang.String)) - Adapt a source table to be used for a tree to ensure that the result will have no orphaned nodes.

### Instance

The following methods can be called on an instance of `TreeTable`:

- `getIdentifierColumn` - Get the name of the identifier column used to create the tree table.
- `getNodeDefinition` - Get the TableDefinition that should be exposed to node table consumers.
- `getParentIdentifierColumn` - Get the name of the parent identifier column used to create the tree table.
- `makeNodeOperationsRecorder` - Create a [`TreeNodeOperationsRecorder`](/core/javadoc/io/deephaven/engine/table/hierarchical/TreeTable.NodeOperationsRecorder.html), which records node-level operations when capturing snapshots.
- `withFilters(filters...)` - Create a new `TreeTable` with the specified [`Filters`](/core/javadoc/io/deephaven/api/filter/Filter.html) applied.
- `withNodeFilterColumns(ColumnNames...)` - Create a new `TreeTable` with the specified `ColumnNames...` designated for node-level filtering.
- `withNodeOperations(nodeOperations)` - Create a new `TreeTable` that will apply the [recorded operations](/core/javadoc/io/deephaven/engine/table/hierarchical/TreeTable.NodeOperationsRecorder.html) to nodes when gathering snapshots.

## Returns

A tree table.

## Examples

The following example creates a tree table from a static table.

```groovy order=result,source
source = emptyTable(100).updateView("ID = i", "Parent = i == 0 ? NULL_INT : (int)(i / 4)")

result = source.tree("ID", "Parent")
```

The following example creates a tree table from a real-time table.

```groovy ticking-table order=null
t1 = emptyTable(10_000).updateView("ID = i", "Parent = i == 0 ? NULL_INT : (int)(i / 10)")
t2 = timeTable("PT00:00:00.01").updateView("I = i % 10_000").lastBy("I")

source = t1.join(t2, "ID = I")

result = source.tree("ID", "Parent")
```

![The above `source` and `result` tables](../../../assets/reference/create/tree-table-realtime.gif)

The following example contains orphan nodes. Two tree tables are created. The first omits orphan nodes, while the second promotes them such that they appear in the tree table.

```groovy order=resultNoOrphans,resultWithOrphans,source
import io.deephaven.engine.table.hierarchical.TreeTable

source = emptyTable(100).updateView("ID = i + 2", "Parent = i == 0 ? NULL_INT : i % 100")

resultNoOrphans = source.tree("ID", "Parent")
resultWithOrphans = TreeTable.promoteOrphans(source, "ID", "Parent").tree("ID", "Parent")
```

## Related documentation

- [`emptyTable`](./emptyTable.md)
- [`join`](../join/join.md)
- [`rollup`](./rollup.md)
- [`timeTable`](./timeTable.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/engine/table/Table.html#tree(java.lang.String,java.lang.String))
