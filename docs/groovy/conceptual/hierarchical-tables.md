---
title: Hierarchical tables
---

A hierarchical table shows a table's rows as a tree that you expand and collapse, so you can start from a summary and drill down only into the parts you care about. Deephaven has two kinds: [rollup tables](../how-to-guides/rollup-table.md), which aggregate rows into levels of groups, and [tree tables](../how-to-guides/tree-table.md), which arrange rows by a parent-child relationship stored in the data. Both are built from an ordinary source table, and both keep updating as that source ticks, so a live feed of millions of rows becomes a few summary rows that stay current. This guide explains what hierarchical tables are, how the two kinds differ, and how they behave with ticking data and in the UI.

## What is a hierarchical table?

A hierarchical table organizes the rows of a source table into nodes that you expand and collapse. The terms it uses are defined in [Shared vocabulary](#shared-vocabulary).

A hierarchical table is not a [`Table`](/core/javadoc/io/deephaven/engine/table/Table.html). The [`rollup`](../reference/table-operations/create/rollup.md) method returns a [`RollupTable`](/core/javadoc/io/deephaven/engine/table/hierarchical/RollupTable.html), and the [`tree`](../reference/table-operations/create/tree.md) method returns a [`TreeTable`](/core/javadoc/io/deephaven/engine/table/hierarchical/TreeTable.html). Neither one supports table operations such as [`where`](../reference/table-operations/filter/where.md), [`update`](../reference/table-operations/select/update.md), or [`naturalJoin`](../reference/table-operations/join/natural-join.md). Run those operations on the source table first, and then create the hierarchical table from the result. A hierarchical table has its own, smaller set of methods, described in [Work with a hierarchical table](#work-with-a-hierarchical-table). Methods that change it return a new hierarchical table, and the original is unchanged.

## Shared vocabulary

Both kinds of hierarchical table use the same terms:

- **Node**: A set of rows that share a parent. In a rollup table, a node holds the rows below one aggregated row. In a tree table, a node holds the rows that name one row as their parent.
- **Root**: The top node. Every other node hangs below a row in the node above it.
- **Children**: The rows in the node directly below a row. When you expand a row in the UI, Deephaven shows its children. When you collapse it, they're hidden again.
- **Leaf**: A row with no children.
- **Depth**: How far a row is below the root. The UI indents each row by its depth.
- **Expansion**: Which rows are showing their children. Expansion is part of what the UI asks the server for, not part of the hierarchical table itself, so two users can view the same table with different rows expanded.
- **Constituents**: In a rollup table created with `includeConstituents` set to `true`, the rows from the source table below each leaf group. Constituent rows have the source table's columns and types.
- **Orphan**: In a tree table, a row whose parent ID is not null but matches no row's ID.

## Rollup tables and tree tables

The two kinds of hierarchical table get their structure from different places. A rollup table creates its hierarchy by aggregating: you name the group-by columns, and Deephaven computes a level of aggregated rows for each one. A tree table finds its hierarchy in the data: every row has an ID, and every row names its parent's ID.

|                                | Rollup table                                                                                          | Tree table                                                                                                                                                                                                                      |
| ------------------------------ | ----------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Created with                   | [`rollup`](../reference/table-operations/create/rollup.md)                                            | [`tree`](../reference/table-operations/create/tree.md)                                                                                                                                                                          |
| Where the hierarchy comes from | The group-by columns. Each level groups by one more column, from left to right.                       | An ID column and a parent column. Each row's parent column holds the ID of its parent row.                                                                                                                                      |
| What each row is               | An aggregated row for one group, or, if you include constituents, a row from the source table         | A row from the source table                                                                                                                                                                                                     |
| The root                       | A single row that aggregates every row in the source table                                            | Not a row. The root node holds every row whose parent is null, so a tree can have many top-level rows.                                                                                                                          |
| Leaves                         | The rows grouped by every group-by column, or, with constituents included, the source rows below them | Rows that no other row names as its parent                                                                                                                                                                                      |
| Requirements                   | Some aggregations aren't supported. See the [rollup how-to guide](../how-to-guides/rollup-table.md).  | IDs must be unique, and the ID and parent columns must have the same type.                                                                                                                                                      |
| Rows left out                  | None                                                                                                  | Orphans, unless you promote them first with [`TreeTable.promoteOrphans`](/core/javadoc/io/deephaven/engine/table/hierarchical/TreeTable.html#promoteOrphans(io.deephaven.engine.table.Table,java.lang.String,java.lang.String)) |

The following example builds both kinds from the same table of employees. `rollupResult` totals the salaries by department and then by team. `treeResult` arranges the employees by who manages whom:

```groovy test-set=hierarchical order=rollupResult,treeResult,employees
employees = newTable(
    intCol("ID", 1, 2, 3, 4, 5, 6),
    intCol("ManagerID", NULL_INT, 1, 1, 2, 2, 3),
    stringCol("Name", "Ana", "Ben", "Cal", "Dee", "Eli", "Fay"),
    stringCol("Dept", "Exec", "Eng", "Sales", "Eng", "Eng", "Sales"),
    stringCol("Team", "Exec", "Core", "East", "Core", "UI", "East"),
    intCol("Salary", 300, 200, 180, 120, 110, 90)
)

rollupResult = employees.rollup([AggSum("Salary"), AggCount("Headcount")], "Dept", "Team")
treeResult = employees.tree("ID", "ManagerID")
```

In `rollupResult`, the top row is the company total. Expanding it shows one row per department, and expanding a department shows its teams. A rollup table keeps every group-by column at every level, so each level has the same columns, and a level that doesn't group by a column shows null in it: the total row shows null in both `Dept` and `Team`, and the department rows show null in `Team`. In `treeResult`, Ana is the only top-level row, because she is the only employee with a null `ManagerID`.

## Work with a hierarchical table

Most of a hierarchical table's methods fall into two groups: filters, and operations that apply to each node.

### Filter

Both kinds of hierarchical table can be filtered, but they filter in different ways:

- [`RollupTable.withFilter`](/core/javadoc/io/deephaven/engine/table/hierarchical/RollupTable.html#withFilter(io.deephaven.api.filter.Filter)) filters the rows before they're aggregated and then recomputes every level, so the totals in the parent rows count only the rows that pass the filter. A filter can use group-by columns and other source columns, but not the columns that aggregations read.
- [`TreeTable.withFilter`](/core/javadoc/io/deephaven/engine/table/hierarchical/TreeTable.html#withFilter(io.deephaven.api.filter.Filter)) keeps the rows that match and every ancestor of each match, so a matching row is still reachable from the root. It doesn't keep the children of a matching row unless they also match.

Neither accepts a filter that reads other rows, such as one that uses a virtual row variable like `i` or a column array. Apply such a filter to the source table first.

The following example filters both tables from the previous example. [`RollupTable.withFilter`](/core/javadoc/io/deephaven/engine/table/hierarchical/RollupTable.html#withFilter(io.deephaven.api.filter.Filter)) and [`TreeTable.withFilter`](/core/javadoc/io/deephaven/engine/table/hierarchical/TreeTable.html#withFilter(io.deephaven.api.filter.Filter)) each take a single [`Filter`](/core/javadoc/io/deephaven/api/filter/Filter.html), so [`Filter.and`](/core/javadoc/io/deephaven/api/filter/Filter.html#and(java.util.Collection)) combines the filters that [`Filter.from`](/core/javadoc/io/deephaven/api/filter/Filter.html#from(java.lang.String...)) parses from strings. In `rollupFiltered`, the total row counts only the two employees on the `Core` team, so it shows a salary of 320 and a headcount of 2. In `treeFiltered`, Dee matches, and her manager Ben and his manager Ana stay so that Dee can still be reached from the root:

```groovy test-set=hierarchical order=rollupFiltered,treeFiltered
import io.deephaven.api.filter.Filter

rollupFiltered = rollupResult.withFilter(Filter.and(Filter.from("Team = `Core`")))
treeFiltered = treeResult.withFilter(Filter.and(Filter.from("Name = `Dee`")))
```

### Apply operations to each node

A _node operations recorder_ holds operations that Deephaven applies to every node: sorting, formatting, and either adding columns (rollup tables) or filtering (tree tables). To use one:

1. Get a recorder from [`RollupTable.makeNodeOperationsRecorder`](/core/javadoc/io/deephaven/engine/table/hierarchical/RollupTable.html#makeNodeOperationsRecorder(io.deephaven.engine.table.hierarchical.RollupTable.NodeType)) or [`TreeTable.makeNodeOperationsRecorder`](/core/javadoc/io/deephaven/engine/table/hierarchical/TreeTable.html#makeNodeOperationsRecorder()).
2. Record operations on it.
3. Pass it to [`RollupTable.withNodeOperations`](/core/javadoc/io/deephaven/engine/table/hierarchical/RollupTable.html#withNodeOperations(io.deephaven.engine.table.hierarchical.RollupTable.NodeOperationsRecorder...)) or [`TreeTable.withNodeOperations`](/core/javadoc/io/deephaven/engine/table/hierarchical/TreeTable.html#withNodeOperations(io.deephaven.engine.table.hierarchical.TreeTable.NodeOperationsRecorder)), which returns a new hierarchical table.

Operations from a recorder apply to each node separately. A sort, for example, orders the children of each row among themselves. It doesn't move a row out from under its parent.

- A [`RollupTable.NodeOperationsRecorder`](/core/javadoc/io/deephaven/engine/table/hierarchical/RollupTable.NodeOperationsRecorder.html) can sort and format each node, and add columns with its [`updateView`](/core/javadoc/io/deephaven/engine/table/hierarchical/RollupTable.NodeOperationsRecorder.html) method. A rollup table's aggregated rows and its constituent rows take separate recorders, chosen with [`RollupTable.NodeType`](/core/javadoc/io/deephaven/engine/table/hierarchical/RollupTable.NodeType.html).
- A [`TreeTable.NodeOperationsRecorder`](/core/javadoc/io/deephaven/engine/table/hierarchical/TreeTable.NodeOperationsRecorder.html) can sort and format each node, and filter it with its [`where`](/core/javadoc/io/deephaven/engine/table/hierarchical/TreeTable.NodeOperationsRecorder.html) method. Unlike [`TreeTable.withFilter`](/core/javadoc/io/deephaven/engine/table/hierarchical/TreeTable.html#withFilter(io.deephaven.api.filter.Filter)), it filters each node on its own and doesn't keep the ancestors of matching rows. [`TreeTable.withNodeFilterColumns`](/core/javadoc/io/deephaven/engine/table/hierarchical/TreeTable.html#withNodeFilterColumns(java.util.Collection)) makes [`withFilter`](/core/javadoc/io/deephaven/engine/table/hierarchical/TreeTable.html#withFilter(io.deephaven.api.filter.Filter)) work the same way for filters that use only the columns you name, which is faster but also drops the ancestors.

A rollup table also has [`withUpdateView`](/core/javadoc/io/deephaven/engine/table/hierarchical/RollupTable.html#withUpdateView(java.lang.String...)), a shortcut that adds columns to the aggregated rows.

## Hierarchical tables and ticking data

A hierarchical table updates as its source table ticks. Deephaven recomputes only the parts that change: a new source row updates the aggregates on its path to the root of a rollup table, or adds a row to one node of a tree table. A hierarchical table built from a [live data feed](./table-update-model.md) therefore stays current without being rebuilt.

Some sources don't work with hierarchical tables:

- **[Blink tables](./table-types.md#specialization-3-blink)** keep only the rows from the current update cycle, so they can't supply the history that some parts of a hierarchy need:
  - [`tree`](../reference/table-operations/create/tree.md) raises an error.
  - [`rollup`](../reference/table-operations/create/rollup.md) raises an error with `includeConstituents` set to `true`, or with [`AggFirst`](../reference/table-operations/group-and-aggregate/AggFirst.md), [`AggLast`](../reference/table-operations/group-and-aggregate/AggLast.md), [`AggSortedFirst`](../reference/table-operations/group-and-aggregate/AggSortedFirst.md), [`AggSortedLast`](../reference/table-operations/group-and-aggregate/AggSortedLast.md), [`AggGroup`](../reference/table-operations/group-and-aggregate/AggGroup.md), or [`AggFormula`](../reference/table-operations/group-and-aggregate/AggFormula.md).
  - Other aggregations, such as a sum or a count, work and summarize every row seen since the rollup started.

  To use any of these, first convert the blink table with [`blinkToAppendOnly`](../reference/table-operations/create/blink-to-append-only.md).

- **Duplicate IDs in a tree**: Each ID in a tree table must be unique. If the source table already has a duplicate ID, [`tree`](../reference/table-operations/create/tree.md) raises an error. If a duplicate ID arrives on a later update, Deephaven reports an error and the tree table can no longer update correctly.

## View and navigate in the UI

The web UI shows a hierarchical table as a grid with an arrow next to each row that has children. Click the arrow to expand or collapse that row. Each level is indented below its parent.

When you open a rollup table, its root row is expanded one level, so you see the grand total and the first level of groups. When you open a tree table, its top-level rows are showing, all collapsed.

When a client such as the web UI displays a ticking hierarchical table, the server sends a fresh copy of every row the client can see whenever the source table changes, not only the rows that changed. It sends these no more often than the subscription's update interval, which defaults to the server's setting of once per second. See [What is Barrage?](./what-is-barrage.md#update-intervals-and-batching) for how the update interval works.

From the JavaScript API, a client works with both kinds through [`dh.TreeTable`](/core/client-api/javascript/classes/dh.TreeTable.html), which can expand, collapse, sort, and filter rows on the server.

## Related documentation

- [Aggregations in Deephaven](./aggregations.md)
- [Incremental update model](./table-update-model.md)
- [Table types](./table-types.md)
- [What is Barrage?](./what-is-barrage.md)
- [Create a hierarchical rollup table](../how-to-guides/rollup-table.md)
- [Create a hierarchical tree table](../how-to-guides/tree-table.md)
- [`rollup`](../reference/table-operations/create/rollup.md)
- [`tree`](../reference/table-operations/create/tree.md)
- [`where`](../reference/table-operations/filter/where.md)
- [`update`](../reference/table-operations/select/update.md)
- [`naturalJoin`](../reference/table-operations/join/natural-join.md)
- [`blinkToAppendOnly`](../reference/table-operations/create/blink-to-append-only.md)
- [`AggFirst`](../reference/table-operations/group-and-aggregate/AggFirst.md)
- [`AggLast`](../reference/table-operations/group-and-aggregate/AggLast.md)
- [`AggSortedFirst`](../reference/table-operations/group-and-aggregate/AggSortedFirst.md)
- [`AggSortedLast`](../reference/table-operations/group-and-aggregate/AggSortedLast.md)
- [`AggGroup`](../reference/table-operations/group-and-aggregate/AggGroup.md)
- [`AggFormula`](../reference/table-operations/group-and-aggregate/AggFormula.md)
- [`Table`](/core/javadoc/io/deephaven/engine/table/Table.html)
- [`RollupTable`](/core/javadoc/io/deephaven/engine/table/hierarchical/RollupTable.html)
- [`TreeTable`](/core/javadoc/io/deephaven/engine/table/hierarchical/TreeTable.html)
- [`Filter`](/core/javadoc/io/deephaven/api/filter/Filter.html)
- [`RollupTable.NodeOperationsRecorder`](/core/javadoc/io/deephaven/engine/table/hierarchical/RollupTable.NodeOperationsRecorder.html)
- [`TreeTable.NodeOperationsRecorder`](/core/javadoc/io/deephaven/engine/table/hierarchical/TreeTable.NodeOperationsRecorder.html)
- [`RollupTable.NodeType`](/core/javadoc/io/deephaven/engine/table/hierarchical/RollupTable.NodeType.html)
- [`dh.TreeTable`](/core/client-api/javascript/classes/dh.TreeTable.html)
