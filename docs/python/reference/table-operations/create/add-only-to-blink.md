---
title: add_only_to_blink
---

The `add_only_to_blink` function converts an [add-only](../../../conceptual/table-types.md#specialization-2-add-only) or [append-only](../../../conceptual/table-types.md#specialization-1-append-only) table to a [blink table](../../../conceptual/table-types.md#specialization-3-blink). At creation, the blink table contains all rows currently in the source table; in each later [update cycle](../../../conceptual/table-update-model.md), it contains only the rows added in that cycle.

> [!NOTE]
> Use this function only on add-only or append-only tables that are not fully in memory, or when [blink-table aggregation semantics](../../../conceptual/table-types.md#specialized-semantics-for-blink-tables) are needed. If the source table holds all its rows in memory, the conversion increases memory use instead of reducing it: the source table still keeps every row, and the blink table adds its own row set and listener.

## Syntax

```python syntax
from deephaven.stream import add_only_to_blink

add_only_to_blink(table: Table) -> Table
```

## Parameters

<ParamTable>
<Param name="table" type="Table">

The add-only or append-only table to convert to a blink table. The table **must** be refreshing; passing a static table raises a `DHError`. The table must also only ever add rows. `add_only_to_blink` does not check this at call time. If the source later removes, modifies, or shifts rows, that update fails with an error.

</Param>
</ParamTable>

## Returns

A blink table. If `table` is already a blink table, `add_only_to_blink` returns it unchanged.

## Example

The following example creates an append-only table with [`time_table`](./timeTable.md), converts it to a blink table with `add_only_to_blink`, and prints the value of [`is_blink`](../metadata/is_blink.md) before and after the conversion.

```python ticking-table order=:log
from deephaven.stream import add_only_to_blink
from deephaven import time_table

tt1 = time_table(period="PT1S")

print(f"Before conversion: {tt1.is_blink}")

blink_result = add_only_to_blink(tt1)

print(f"After conversion: {blink_result.is_blink}")
```

The animation shows `tt1` keeping every row it has added, while `blink_result` holds only the rows added in the current update cycle.

![The above `tt1` and `blink_result` tables ticking side by side in the Deephaven console](../../../assets/reference/create/add-only-to-blink.gif)

A time table computes each timestamp from its row key and stores no per-row data, so converting it saves no memory; the example only illustrates the syntax. To create a blink time table directly, pass `blink_table=True` to [`time_table`](./timeTable.md), as shown in [Create a blink time table](../../../how-to-guides/time-table.md#create-a-blink-time-table).

## Related documentation

- [Convert add- and append-only tables to blink tables](../../../how-to-guides/add-only-to-blink.md)
- [`time_table`](./timeTable.md)
- [Create a blink time table](../../../how-to-guides/time-table.md#create-a-blink-time-table)
- [`is_blink`](../metadata/is_blink.md)
- [`remove_blink`](./remove-blink.md)
- [`blink_to_append_only`](./blink-to-append-only.md)
- [Table types: Specialized semantics for blink tables](../../../conceptual/table-types.md#specialized-semantics-for-blink-tables)
- [Incremental update model](../../../conceptual/table-update-model.md)
- [Pydoc](/core/pydoc/code/deephaven.stream.html#deephaven.stream.add_only_to_blink)
