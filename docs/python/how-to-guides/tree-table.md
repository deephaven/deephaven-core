---
title: Create a hierarchical tree table
sidebar_label: Tree tables
---

This guide will show you how to create a hierarchical tree table. A tree table is a table with an expandable [tree structure](https://en.wikipedia.org/wiki/Tree_(data_structure)), as seen in the diagram below:

![A diagram of a hierarchical tree structure with parent and child nodes](../assets/how-to/new-tree2.png)

In computer science, trees are data structures used to represent hierarchical relationships between pieces of data. The data within the tree is stored in _nodes_, which are represented by the boxes in the diagram above.

Every tree table has one (and only one) root node, which is the topmost node in the tree. Aside from the root node, all other nodes in the tree follow the same rules: each node can have one (and only one) parent, and zero or more children. In the diagram above, `B7`'s parent is `A3`, and its children are `C1` and `C2`. Nodes with no children are known as _leaf nodes_, or leaves, as they are the terminal nodes of the tree structure. `B3` and `C1` are both leaves in the diagram above.

A node with no parent is known as an _orphan_, and will appear in the table outside of the tree structure.

## `tree`

In Deephaven, tree tables are created using the `tree` method. This method takes three arguments:

```python syntax
result = source.tree(id_col, parent_col, promote_orphans)
```

Where:

- `id_col` is the name of the column that contains the unique identifier for each node in the tree.
- `parent_col` is the name of the column that contains the unique identifier for the parent of each node in the tree.
- `promote_orphans` is an optional boolean that determines whether nodes with no parent should be promoted to be children of the root node rather than appearing outside of the tree structure. By default, this is set to `False`.

The resulting table is initially collapsed, only showing the root node. Clicking on that node will expand it to show its children, and so on. Rows in the initial table with a `parent_col` value equal to a row in the `id_col` column will appear as children of the parent row.

![A user expands notes in a tree table](../assets/how-to/treetable.gif)

## Examples

### Static data

The first example creates two constituent tables, which are then [joined](../reference/table-operations/join/join.md) together to form the `source` table. The `ID` and `Parent` columns in `source` are used as the ID and parent columns, respectively.

```python order=result,source
from deephaven.constants import NULL_INT
from deephaven import empty_table

source = empty_table(100).update_view(
    ["ID = i", "Parent = i == 0 ? NULL_INT : (int)(i / 4)"]
)

result = source.tree(id_col="ID", parent_col="Parent")
```

### Real-time data

Tree tables work in real-time applications the same way as they do in static contexts. This can be shown via an example similar to the one above.

```python ticking-table order=null
from deephaven import empty_table, time_table
from deephaven.constants import NULL_INT

t1 = empty_table(10_000).update_view(
    ["ID = i", "Parent = i == 0 ? NULL_INT : (int)(i / 10)"]
)
t2 = time_table("PT0.01S").update_view(["I = i % 10_000"]).last_by("I")

source = t1.join(t2, "ID = I")

result = source.tree(id_col="ID", parent_col="Parent")
```

![The above real-time tree table](../assets/reference/create/tree-table-realtime.gif)

## Orphan nodes

Rows (nodes) in a tree table are considered "orphans" if:

- The node in question is not the root node.
- The node's parent is _not_ null.
- The node's parent does not exist in the table.

Non-root nodes where the parent is `null` _are not considered orphans_. They can appear in the tree table, but they will appear outside of the tree structure and will be unaffected by the `promote_orphans` argument. For example, see rows 102 and 103 in the following figure:

![A tree table with orphan nodes](../assets/how-to/tree-null-parents.png)

Note that these rows appear on the same level as the root node, outside of the tree structure.

Orphan nodes appear outside of the tree table's tree structure by default. In order to include orphans in a tree table as children of the root node, the optional argument `promote_orphans` can be switched to `True`.

The following example shows how the resulting tree table changes if orphans are promoted.

```python order=result_no_orphans,result_w_orphans,source
from deephaven.constants import NULL_INT
from deephaven import empty_table

source = empty_table(100).update_view(
    ["ID = i + 2", "Parent = i == 0 ? NULL_INT : i % 9"]
)

result_no_orphans = source.tree(id_col="ID", parent_col="Parent")
result_w_orphans = source.tree(id_col="ID", parent_col="Parent", promote_orphans=True)
```

## Related documentation

- [How to create a hierarchical rollup table](./rollup-table.md)
- [How to create an empty table](../how-to-guides/new-and-empty-table.md#empty_table)
- [How to create a time table](../how-to-guides/time-table.md)
- [Joins: Exact and Relational](../how-to-guides/joins-exact-relational.md)
- [Joins: Time-Series and Range](../how-to-guides/joins-timeseries-range.md)
- [tree](../reference/table-operations/create/tree.md)
