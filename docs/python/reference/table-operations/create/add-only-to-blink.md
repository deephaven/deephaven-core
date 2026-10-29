---
title: add_only_to_blink
---

The `add_only_to_blink` method creates a blink table from the supplied add-only or append-only table. The blink table contains the rows added in the latest update cycle.

> [!NOTE]
> Use this function only on add-only or append-only tables that are not fully in memory, or when you want blink table aggregation semantics. Creating a downstream blink table from a table that is fully in memory is not recommended. It does not achieve the main benefit of blink tables, which is lower memory use. Instead, it increases memory use.

## Syntax

```python syntax
from deephaven.stream import add_only_to_blink

add_only_to_blink(table: Table) -> Table
```

## Parameters

<ParamTable>
<Param name="table" type="Table">

The add-only or append-only table to convert to a blink table. The table **must** be refreshing.

</Param>
</ParamTable>

## Returns

A blink table.

## Example

The following example creates an append-only table with [`time_table`](./timeTable.md) and then converts it to a blink table with `add_only_to_blink`. This example uses `add_only_to_blink` on an in-memory table, which is not recommended, but it illustrates the syntax of the method.

```python ticking-table order=null
from deephaven.stream import add_only_to_blink
from deephaven import time_table

tt1 = time_table(period="PT1S")

blink_result = add_only_to_blink(tt1)
```

![The above `tt1` and `blink_result` tables ticking side-by-side in the Deephaven console](../../../assets/reference/create/add-only-to-blink.gif)

## Related documentation

- [`time_table`](./timeTable.md)
- [`is_blink`](../metadata/is_blink.md)
- [`remove_blink`](./remove-blink.md)
- [`blink_to_append_only`](./blink-to-append-only.md)
- [Table types: Specialized semantics for blink tables](../../../conceptual/table-types.md#specialized-semantics-for-blink-tables)
- [Pydoc](/core/pydoc/code/deephaven.stream.html#deephaven.stream.add_only_to_blink)
