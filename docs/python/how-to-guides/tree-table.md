---
title: Create a hierarchical tree table
---

This guide will show you how to create a hierarchical tree table. A tree table is a table with an expandable [tree structure](https://en.wikipedia.org/wiki/Tree_(data_structure)), as seen in the diagram below:

![A diagram of a hierarchical tree structure with parent and child nodes](../assets/how-to/new-tree2.png)

In computer science, trees are data structures used to represent hierarchical relationships between pieces of data. The data within the tree is stored in _nodes_, which are represented by the boxes in the diagram above.

In a Deephaven tree table, the root node (`Root` in the diagram above) is implicit: its children are the rows whose parent column is null, and these rows are the top-level nodes of the tree. Every other node has one (and only one) parent, and any node can have zero or more children. In the diagram above, `B7`'s parent is `A3`, and its children are `C1` and `C2`. Nodes with no children are known as _leaf nodes_, or leaves, as they are the terminal nodes of the tree structure. `B3` and `C1` are both leaves in the diagram above.

A node whose parent ID is not null but does not match any row's ID is an _orphan_. Orphans are left out of the tree.

## `tree`

In Deephaven, tree tables are created using the `tree` method:

```python syntax
result = source.tree(id_col, parent_col, promote_orphans)
```

Where:

- `id_col` is the name of the column that contains the unique identifier for each node in the tree.
- `parent_col` is the name of the column that contains the unique identifier for the parent of each node in the tree.
- `promote_orphans` is an optional boolean that determines whether orphan nodes (rows whose non-null parent does not exist in the table) are promoted to children of the root node instead of being left out of the tree. By default, this is set to `False`.

The resulting table initially shows only the top-level rows (rows with a null parent), collapsed. Click a row to expand it and show its children, and so on. Rows in the initial table with a `parent_col` value equal to a row in the `id_col` column will appear as children of the parent row.

![A user expands notes in a tree table](../assets/how-to/treetable.gif)

## Examples

### Static data

The first example creates a `source` table in which each row's `Parent` is its `ID` divided by 4, rounded down (row 0 has a null parent), and builds a tree from the `ID` and `Parent` columns.

```python order=result,source
from deephaven.constants import NULL_INT
from deephaven import empty_table

source = empty_table(100).update_view(
    ["ID = i", "Parent = i == 0 ? NULL_INT : (int)(i / 4)"]
)

result = source.tree(id_col="ID", parent_col="Parent")
```

### Real-time data

Tree tables work in real-time applications the same way as they do in static contexts. This can be shown via an example similar to the one above. It creates two constituent tables, which are then [joined](../reference/table-operations/join/join.md) together to form the `source` table.

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

![Animated GIF showing how a tree table updates in real time as data changes](../assets/reference/create/tree-table-realtime.gif)

## Orphan nodes

Rows (nodes) in a tree table are considered "orphans" if:

- The node's parent is _not_ null.
- The node's parent does not exist in the table.

Rows whose parent is null are not orphans. They are top-level nodes of the tree, like rows 102 and 103 in the following figure.

![A tree table in which rows 102 and 103, which have null parents, appear as top-level nodes](../assets/how-to/tree-null-parents.png)

By default, orphan nodes don't appear in the tree table. To include orphans in a tree table as children of the root node (top-level nodes, like rows with a null parent), switch the optional argument `promote_orphans` to `True`.

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
- [`tree`](../reference/table-operations/create/tree.md)
