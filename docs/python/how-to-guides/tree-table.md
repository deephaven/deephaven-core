---
title: Create a hierarchical tree table
---

This guide shows you how to create a hierarchical tree table from a table whose rows reference each other through an ID column and a parent column. A tree table is a table with an expandable [tree structure](https://en.wikipedia.org/wiki/Tree_(data_structure)), as seen in the diagram below:

![A diagram of a hierarchical tree structure with parent and child nodes](../assets/how-to/new-tree2.png)

In computer science, trees are data structures used to represent hierarchical relationships between pieces of data. A tree stores its data in _nodes_. Each box in the diagram above is a node.

Every node except the root has one parent, and any node can have zero or more children. In the diagram above, `B7`'s parent is `A3`, and its children are `C1` and `C2`.

In a Deephaven tree table, the `Root` node at the top of the diagram is implicit and doesn't correspond to a row. Its children, like `A1`, `A2`, and `A3`, are the rows whose parent is null. These rows are the top-level nodes of the tree.

A row whose parent is missing from the table, like `Orphan` in the diagram, is an _orphan_. See [Orphan nodes](#orphan-nodes).

If your rows don't reference each other and you instead want to group rows by column values and aggregate each group, use a [rollup table](./rollup-table.md).

## `tree`

Create a tree table with the [`tree`](../reference/table-operations/create/tree.md) method:

```python syntax
result = source.tree(id_col, parent_col, promote_orphans)
```

Where:

- `id_col` is the name of the column that contains the unique identifier for each node in the tree.
- `parent_col` is the name of the column that contains the identifier of each node's parent. Rows with a null parent are top-level nodes.
- `promote_orphans` is an optional boolean. When it is `True`, the tree shows [orphans](#orphan-nodes) as top-level nodes instead of leaving them out. The default is `False`.

Each row whose `parent_col` value matches another row's `id_col` value is a child of that row.

The resulting table initially shows only the top-level rows, collapsed. Click a row to expand it and show its children.

![A user expands nodes in a tree table](../assets/how-to/treetable.gif)

## Examples

### Static data

The following example creates a `source` table in which each row's `Parent` is its `ID` divided by 4, rounded down. Row 0 has a null parent, so it is the only top-level node. The example then builds a tree from the `ID` and `Parent` columns.

```python order=result,source
from deephaven import empty_table

source = empty_table(100).update_view(
    ["ID = i", "Parent = i == 0 ? NULL_INT : (int)(i / 4)"]
)

result = source.tree(id_col="ID", parent_col="Parent")
```

### Real-time data

Tree tables work with [ticking](../conceptual/table-types.md) data the same way they work with static data, except that `tree` doesn't support [blink tables](../conceptual/table-types.md#specialization-3-blink). The following example builds the same kind of `ID`/`Parent` hierarchy as the [static data example](#static-data), with 10,000 rows. Each row's `Parent` is its `ID` divided by 10, rounded down.

It then creates a [time table](../reference/table-operations/create/timeTable.md) that adds one new `I` value every 10 milliseconds. After all 10,000 values have appeared, `I` wraps back to 0, and [`last_by`](../reference/table-operations/group-and-aggregate/lastBy.md) keeps only the latest row for each `I`. [Joining](../reference/table-operations/join/join.md) the first table to the time table produces `source`, which contains only the rows whose `ID` matches an `I` value from the time table, so the tree grows as rows arrive. After the wrap, the tree updates in place instead of growing.

```python ticking-table order=null
from deephaven import empty_table, time_table

t1 = empty_table(10_000).update_view(
    ["ID = i", "Parent = i == 0 ? NULL_INT : (int)(i / 10)"]
)
t2 = time_table("PT0.01S").update_view(["I = i % 10_000"]).last_by("I")

source = t1.join(t2, "ID = I")

result = source.tree(id_col="ID", parent_col="Parent")
```

![Animated GIF showing how a tree table updates in real time as data changes](../assets/reference/create/tree-table-realtime.gif)

## Orphan nodes

A row in a tree table is an orphan if both of the following are true:

- The row's parent is _not_ null.
- No row in the table has that parent's ID.

By default, orphans and all of their descendants don't appear in the tree table. To include them, set the `promote_orphans` argument of [`tree`](../reference/table-operations/create/tree.md) to `True`. Promoted orphans become top-level nodes alongside the rows whose parent is null. Their descendants appear beneath them.

In the following example, `source` has `ID` values from 2 to 103. Rows 2, 102, and 103 have a null parent. Every other row's parent is a number from 0 to 8, so the rows whose parent is 0 or 1 are orphans. Some orphans, such as row 3, have descendants of their own (rows 5, 7, 9, and others). `result_no_orphans` leaves out the orphans and their descendants, and `result_with_orphans` shows the orphans as top-level nodes with their descendants beneath them.

```python order=result_no_orphans,result_with_orphans,source
from deephaven import empty_table, merge

source = merge(
    [
        empty_table(100).update_view(
            ["ID = i + 2", "Parent = i == 0 ? NULL_INT : i % 9"]
        ),
        empty_table(2).update_view(["ID = i + 102", "Parent = NULL_INT"]),
    ]
)

result_no_orphans = source.tree(id_col="ID", parent_col="Parent")
result_with_orphans = source.tree(
    id_col="ID", parent_col="Parent", promote_orphans=True
)
```

The following figure shows the end of `source` next to `result_no_orphans`. Rows 2, 102, and 103 are top-level nodes. Orphans such as rows 92, 93, and 101, whose parents are 0 or 1, don't appear in the tree.

![The source table next to a tree table. Rows 2, 102, and 103 are top-level nodes, and orphan rows 92, 93, and 101 are absent](../assets/how-to/tree-null-parents.png)

## Related documentation

- [How to create a hierarchical rollup table](./rollup-table.md)
- [How to create an empty table](../how-to-guides/new-and-empty-table.md#empty_table)
- [How to create a time table](../how-to-guides/time-table.md)
- [Joins: Exact and Relational](../how-to-guides/joins-exact-relational.md)
- [Joins: Time-Series and Range](../how-to-guides/joins-timeseries-range.md)
- [`merge`](../reference/table-operations/merge/merge.md)
- [`tree`](../reference/table-operations/create/tree.md)
