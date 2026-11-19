---
title: tree
---

The Deephaven `tree` method creates a tree table from a source table given an ID column name and parent column name.

`tree` does not support a [blink table](../../../conceptual/table-types.md#specialization-3-blink) source and raises an error if given one. To build a tree from a blink table, first convert it to an append-only table with [`blink_to_append_only`](./blink-to-append-only.md).

## Syntax

```python syntax
source.tree(id_col: str, parent_col: str, promote_orphans: bool = False) -> TreeTable
```

## Parameters

<ParamTable>
<Param name="id_col" type="str">

The name of the ID column. Each ID must be unique. If the source table has a duplicate ID, `tree` raises an error.

</Param>
<Param name="parent_col" type="str">

The name of the parent column. It must have the same type as the ID column.

</Param>
<Param name="promote_orphans" type="bool" optional>

Whether to promote node tables whose parents do not exist to be children of the root node. The default is `False`.

</Param>
</ParamTable>

## Returns

A tree table.

## Examples

The following example creates a tree table from a static table.

```python order=result,source
from deephaven import empty_table

source = empty_table(100).update_view(
    ["ID = i", "Parent = i == 0 ? NULL_INT : (int)(i / 4)"]
)

result = source.tree(id_col="ID", parent_col="Parent")
```

The following example creates a tree table from a real-time table.

```python ticking-table order=null
from deephaven import empty_table, time_table

t1 = empty_table(10_000).update_view(
    ["ID = i", "Parent = i == 0 ? NULL_INT : (int)(i / 10)"]
)
t2 = time_table("PT0.01S").update_view(["I = i % 10_000"]).last_by("I")

source = t1.join(t2, "ID = I")

result = source.tree(id_col="ID", parent_col="Parent")
```

![A user navigates between the `source` table and `result` tree table](../../../assets/reference/create/tree-table-realtime.gif)

The following example creates a tree table from a source table that contains orphans. The first omits orphan nodes, while the second promotes them such that they appear in the tree table.

```python order=result_no_orphans,result_w_orphans,source
from deephaven import empty_table

source = empty_table(100).update_view(
    ["ID = i + 2", "Parent = i == 0 ? NULL_INT : i % 9"]
)

result_no_orphans = source.tree(id_col="ID", parent_col="Parent")
result_w_orphans = source.tree(id_col="ID", parent_col="Parent", promote_orphans=True)
```

## Related documentation

- [Table types](../../../conceptual/table-types.md)
- [`blink_to_append_only`](./blink-to-append-only.md)
- [`empty_table`](./emptyTable.md)
- [`time_table`](./timeTable.md)
- [`join`](../join/join.md)
- [`rollup`](./rollup.md)
- [Javadoc](/core/javadoc/io/deephaven/engine/table/Table.html#tree(java.lang.String,java.lang.String))
- [Pydoc](/core/pydoc/code/deephaven.table.html#deephaven.table.Table.tree)
- [TreeTable](/core/pydoc/code/deephaven.table.html#deephaven.table.TreeTable)
