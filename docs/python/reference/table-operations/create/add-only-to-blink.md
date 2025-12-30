---
title: add_only_to_blink
---

The `add_only_to_blink` method creates a blink table from the supplied add-only table. The blink table contains the rows added in the latest update cycle.

> [!NOTE]
> The use of this function should be limited to add-only tables that are not fully in-memory, or when blink-table specific aggregation semantics are desired. If the table is fully in-memory, creating a downstream blink table is not recommended because it doesn't achieve the main benefit of blink tables, which is to reduce memory usage but instead increases memory usage.

## Syntax

```python syntax
from deephaven.stream import add_only_to_blink

add_only_to_blink(table: Table) -> Table
```

## Parameters

<ParamTable>
<Param name="table" type="Table">

The add-only table to convert to a blink table.

</Param>
</ParamTable>

## Returns

A blink table.

## Example

The following example creates an add-only table with [`time_table`](./timeTable.md) and then converts it to a blink table with `add_only_to_blink`. This example uses `add_only_to_blink` on an in-memory table, which is not recommended, but it illustrates the syntax of the method.

```python order=null
from deephaven.stream import add_only_to_blink
from deephaven import time_table

tt1 = time_table(period="PT1S")

blink_result = add_only_to_blink(tt1)
```

![The above `tt1` and `blink_result` tables ticking side-by-side in the Deephaven console](../../../assets/reference/create/add-only-to-blink.gif)

## Related Documentation

- [`time_table`](./timeTable.md)
- [Pydoc](/core/pydoc/code/deephaven.stream.html#deephaven.stream.add_only_to_blink)
