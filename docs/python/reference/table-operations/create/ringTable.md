---
title: ring_table
---

The `ring_table` method creates a ring table that retains the latest `capacity` number of rows from the parent table.
Latest rows are determined by the new rows added to the parent table. Deleted rows are ignored, and updated rows are not expected and will raise an exception.

## Syntax

```python syntax
ring_table(parent: Table, capacity: int, initialize: bool = True) -> Table
```

## Parameters

<ParamTable>
<Param name="parent" type="Table">

The parent table.

</Param>
<Param name="capacity" type="int">

The capacity of the ring table.

</Param>
<Param name="initialize" type="bool">

Whether to initialize the ring table with a snapshot of the parent table. Default is `True`.

</Param>
</ParamTable>

## Returns

An in-memory ring table.

## Examples

The following example creates a table with one column of three integer values.

```python order=source,result
from deephaven import new_table, ring_table
from deephaven.column import int_col

source = new_table([int_col("IntegerColumn", [1, 2, 3, 4, 5])])

result = ring_table(source, 3)
```

## Related documentation

- [How to create static tables](../../../how-to-guides/new-and-empty-table.md)
- [`empty_table`](./emptyTable.md)
- [`int_col`](./intCol.md)
- [Pydoc](/core/pydoc/code/deephaven.html#deephaven.ring_table)
