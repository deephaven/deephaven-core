---
title: Hierarchical tables
---

A hierarchical table shows a table's rows as a tree that you expand and collapse, so you can start from a summary and drill down only into the parts you care about. Deephaven has two kinds: [rollup tables](../how-to-guides/rollup-table.md), which aggregate rows into levels of groups, and [tree tables](../how-to-guides/tree-table.md), which arrange rows by a parent-child relationship stored in the data. Both are built from an ordinary source table, and both keep updating as that source ticks, so a live feed of millions of rows becomes a few summary rows that stay current. This guide explains what hierarchical tables are, how the two kinds differ, and how they behave with ticking data and in the UI.

## What is a hierarchical table?

A hierarchical table organizes the rows of a source table into nodes that you expand and collapse. The terms it uses are defined in [Shared vocabulary](#shared-vocabulary).

A hierarchical table is not a [`Table`](/core/pydoc/code/deephaven.table.html#deephaven.table.Table). The [`rollup`](../reference/table-operations/create/rollup.md) method returns a [`RollupTable`](/core/pydoc/code/deephaven.table.html#deephaven.table.RollupTable), and the [`tree`](../reference/table-operations/create/tree.md) method returns a [`TreeTable`](/core/pydoc/code/deephaven.table.html#deephaven.table.TreeTable). Neither one supports table operations such as [`where`](../reference/table-operations/filter/where.md), [`update`](../reference/table-operations/select/update.md), or [`natural_join`](../reference/table-operations/join/natural-join.md). Run those operations on the source table first, and then create the hierarchical table from the result. A hierarchical table has its own, smaller set of methods, described in [Work with a hierarchical table](#work-with-a-hierarchical-table). Methods that change it return a new hierarchical table, and the original is unchanged.

## Shared vocabulary

Both kinds of hierarchical table use the same terms:

- **Node**: A set of rows that share a parent. In a rollup table, a node holds the rows below one aggregated row. In a tree table, a node holds the rows that name one row as their parent.
- **Root**: The top node. Every other node hangs below a row in the node above it.
- **Children**: The rows in the node directly below a row. When you expand a row in the UI, Deephaven shows its children. When you collapse it, they're hidden again.
- **Leaf**: A row with no children.
- **Depth**: How far a row is below the root. The UI indents each row by its depth.
- **Expansion**: Which rows are showing their children. Expansion is part of what the UI asks the server for, not part of the hierarchical table itself, so two users can view the same table with different rows expanded.
- **Constituents**: In a rollup table created with `include_constituents=True`, the rows from the source table below each leaf group. Constituent rows have the source table's columns and types.
- **Orphan**: In a tree table, a row whose parent ID is not null but matches no row's ID.

## Rollup tables and tree tables

The two kinds of hierarchical table get their structure from different places. A rollup table creates its hierarchy by aggregating: you name the group-by columns, and Deephaven computes a level of aggregated rows for each one. A tree table finds its hierarchy in the data: every row has an ID, and every row names its parent's ID.

|                                | Rollup table                                                                                          | Tree table                                                                                                                         |
| ------------------------------ | ----------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------- |
| Created with                   | [`rollup`](../reference/table-operations/create/rollup.md)                                            | [`tree`](../reference/table-operations/create/tree.md)                                                                             |
| Where the hierarchy comes from | The group-by columns. Each level groups by one more column, from left to right.                       | An ID column and a parent column. Each row's parent column holds the ID of its parent row.                                         |
| What each row is               | An aggregated row for one group, or, if you include constituents, a row from the source table         | A row from the source table                                                                                                        |
| The root                       | A single row that aggregates every row in the source table                                            | Not a row. The root node holds every row whose parent is null, so a tree can have many top-level rows.                             |
| Leaves                         | The rows grouped by every group-by column, or, with constituents included, the source rows below them | Rows that no other row names as its parent                                                                                         |
| Requirements                   | Some aggregations aren't supported. See the [rollup how-to guide](../how-to-guides/rollup-table.md).  | IDs must be unique, and the ID and parent columns must have the same type.                                                         |
| Rows left out                  | None                                                                                                  | Orphans and their descendants, unless you [promote them](../reference/table-operations/create/tree.md) with `promote_orphans=True` |

The following example builds both kinds from the same table of employees. `rollup_result` totals the salaries by department and then by team. `tree_result` arranges the employees by who manages whom:

```python test-set=hierarchical order=rollup_result,tree_result,employees
from deephaven import new_table, agg
from deephaven.column import int_col, string_col

employees = new_table(
    [
        int_col("ID", [1, 2, 3, 4, 5, 6]),
        int_col("ManagerID", [None, 1, 1, 2, 2, 3]),
        string_col("Name", ["Ana", "Ben", "Cal", "Dee", "Eli", "Fay"]),
        string_col("Dept", ["Exec", "Eng", "Sales", "Eng", "Eng", "Sales"]),
        string_col("Team", ["Exec", "Core", "East", "Core", "UI", "East"]),
        int_col("Salary", [300, 200, 180, 120, 110, 90]),
    ]
)

rollup_result = employees.rollup(
    aggs=[agg.sum_("Salary"), agg.count_("Headcount")], by=["Dept", "Team"]
)
tree_result = employees.tree(id_col="ID", parent_col="ManagerID")
```

In `rollup_result`, the top row is the company total. Expanding it shows one row per department, and expanding a department shows its teams. A rollup table keeps every group-by column at every level, so each level has the same columns, and a level that doesn't group by a column shows null in it: the total row shows null in both `Dept` and `Team`, and the department rows show null in `Team`. In `tree_result`, Ana is the only top-level row, because she is the only employee with a null `ManagerID`.

## Work with a hierarchical table

Most of a hierarchical table's methods fall into two groups: filters, and operations that apply to each node.

### Filter

Both kinds of hierarchical table can be filtered, but they filter in different ways:

- [`RollupTable.with_filters`](/core/pydoc/code/deephaven.table.html#deephaven.table.RollupTable.with_filters) filters the rows before they're aggregated and then recomputes every level, so the totals in the parent rows count only the rows that pass the filter. A filter can use group-by columns and other source columns, but not a column that an aggregation summarizes, such as `Salary` in [`agg.sum_("Salary")`](../reference/table-operations/group-and-aggregate/AggSum.md).
- [`TreeTable.with_filters`](/core/pydoc/code/deephaven.table.html#deephaven.table.TreeTable.with_filters) keeps the rows that match and every ancestor of each match, so a matching row is still reachable from the root. It doesn't add a matching row's descendants, so a row below a match stays only if it also matches or is an ancestor of another match.

Neither accepts a filter that reads other rows, such as one that uses a virtual row variable like `i` or a column array. Apply such a filter to the source table first.

The following example filters both tables from the previous example. In `rollup_filtered`, the total row counts only the two employees on the `Core` team, so it shows a salary of 320 and a headcount of 2. In `tree_filtered`, Dee matches, and her manager Ben and his manager Ana stay so that Dee can still be reached from the root:

```python test-set=hierarchical order=rollup_filtered,tree_filtered
rollup_filtered = rollup_result.with_filters("Team = `Core`")
tree_filtered = tree_result.with_filters("Name = `Dee`")
```

### Apply operations to each node

A _node operations recorder_ holds operations that Deephaven applies to every node: sorting, formatting, and either adding columns (rollup tables) or filtering (tree tables). To use one:

1. Get a recorder from [`RollupTable.node_operation_recorder`](/core/pydoc/code/deephaven.table.html#deephaven.table.RollupTable.node_operation_recorder) or [`TreeTable.node_operation_recorder`](/core/pydoc/code/deephaven.table.html#deephaven.table.TreeTable.node_operation_recorder).
2. Record operations on it.
3. Pass it to [`RollupTable.with_node_operations`](/core/pydoc/code/deephaven.table.html#deephaven.table.RollupTable.with_node_operations) or [`TreeTable.with_node_operations`](/core/pydoc/code/deephaven.table.html#deephaven.table.TreeTable.with_node_operations), which returns a new hierarchical table.

Operations from a recorder apply to each node separately. A sort, for example, orders the children of each row among themselves. It doesn't move a row out from under its parent.

- A [`RollupNodeOperationsRecorder`](/core/pydoc/code/deephaven.table.html#deephaven.table.RollupNodeOperationsRecorder) can sort and format each node, and add columns with its [`update_view`](/core/pydoc/code/deephaven.table.html#deephaven.table.RollupNodeOperationsRecorder) method. A rollup table's aggregated rows and its constituent rows take separate recorders, chosen with [`NodeType`](/core/pydoc/code/deephaven.table.html#deephaven.table.NodeType).
- A [`TreeNodeOperationsRecorder`](/core/pydoc/code/deephaven.table.html#deephaven.table.TreeNodeOperationsRecorder) can sort and format each node, and filter it with its [`where`](/core/pydoc/code/deephaven.table.html#deephaven.table.TreeNodeOperationsRecorder) method. Unlike [`TreeTable.with_filters`](/core/pydoc/code/deephaven.table.html#deephaven.table.TreeTable.with_filters), it filters each node on its own and doesn't keep the ancestors of matching rows.

A rollup table also has [`with_update_view`](/core/pydoc/code/deephaven.table.html#deephaven.table.RollupTable.with_update_view), a shortcut that adds columns to the aggregated rows.

## Hierarchical tables and ticking data

A hierarchical table updates as its source table ticks. Deephaven recomputes only the parts that change: a new source row updates the aggregates on its path to the root of a rollup table, or adds a row to one node of a tree table. A hierarchical table built from a [live data feed](./table-update-model.md) therefore stays current without being rebuilt.

Some sources don't work with hierarchical tables:

- **[Blink tables](./table-types.md#specialization-3-blink)** keep only the rows from the current update cycle, so they can't supply the history that some parts of a hierarchy need:
  - [`tree`](../reference/table-operations/create/tree.md) raises an error.
  - [`rollup`](../reference/table-operations/create/rollup.md) raises an error with `include_constituents=True`, or with [`agg.first`](../reference/table-operations/group-and-aggregate/AggFirst.md), [`agg.last`](../reference/table-operations/group-and-aggregate/AggLast.md), [`agg.sorted_first`](../reference/table-operations/group-and-aggregate/AggSortedFirst.md), [`agg.sorted_last`](../reference/table-operations/group-and-aggregate/AggSortedLast.md), [`agg.group`](../reference/table-operations/group-and-aggregate/AggGroup.md), or [`agg.formula`](../reference/table-operations/group-and-aggregate/AggFormula.md).
  - Other aggregations, such as a sum or a count, work and summarize every row seen since the rollup started.

  To use any of these, first convert the blink table with [`blink_to_append_only`](../reference/table-operations/create/blink-to-append-only.md).

- **Duplicate IDs in a tree**: Each ID in a tree table must be unique. If the source table already has a duplicate ID, [`tree`](../reference/table-operations/create/tree.md) raises an error. If a duplicate ID arrives on a later update, Deephaven reports an error and the tree table can no longer update correctly.

## View and navigate in the UI

The web UI shows a hierarchical table as a grid with an arrow next to each row that has children. Click the arrow to expand or collapse that row. Each level is indented below its parent.

When you open a rollup table, its root row is expanded one level, so you see the grand total and the first level of groups. When you open a tree table, its top-level rows are showing, all collapsed.

When a client such as the web UI displays a ticking hierarchical table, the server sends a fresh copy of every row the client can see whenever the source table changes, not only the rows that changed. It sends these no more often than the subscription's update interval. The JavaScript API requests an interval of one second unless the client asks for a different one. The server's default interval, also one second, applies only to a subscription that doesn't request one. See [What is Barrage?](./what-is-barrage.md#update-intervals-and-batching) for how the update interval works.

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
- [`natural_join`](../reference/table-operations/join/natural-join.md)
- [`blink_to_append_only`](../reference/table-operations/create/blink-to-append-only.md)
- [`agg.sum_`](../reference/table-operations/group-and-aggregate/AggSum.md)
- [`agg.first`](../reference/table-operations/group-and-aggregate/AggFirst.md)
- [`agg.last`](../reference/table-operations/group-and-aggregate/AggLast.md)
- [`agg.sorted_first`](../reference/table-operations/group-and-aggregate/AggSortedFirst.md)
- [`agg.sorted_last`](../reference/table-operations/group-and-aggregate/AggSortedLast.md)
- [`agg.group`](../reference/table-operations/group-and-aggregate/AggGroup.md)
- [`agg.formula`](../reference/table-operations/group-and-aggregate/AggFormula.md)
- [`Table`](/core/pydoc/code/deephaven.table.html#deephaven.table.Table)
- [`RollupTable`](/core/pydoc/code/deephaven.table.html#deephaven.table.RollupTable)
- [`TreeTable`](/core/pydoc/code/deephaven.table.html#deephaven.table.TreeTable)
- [`RollupNodeOperationsRecorder`](/core/pydoc/code/deephaven.table.html#deephaven.table.RollupNodeOperationsRecorder)
- [`TreeNodeOperationsRecorder`](/core/pydoc/code/deephaven.table.html#deephaven.table.TreeNodeOperationsRecorder)
- [`NodeType`](/core/pydoc/code/deephaven.table.html#deephaven.table.NodeType)
- [`dh.TreeTable`](/core/client-api/javascript/classes/dh.TreeTable.html)
