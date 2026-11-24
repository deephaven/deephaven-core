---
title: ring_table
---

The `ring_table` function creates a [ring table](../../../conceptual/table-types.md#specialization-4-ring) that retains the latest `capacity` number of rows from the parent table.
Only rows added to the parent table count toward the latest rows.

Ring tables are most often used with [blink tables](../../../conceptual/table-types.md#specialization-3-blink), which retain rows for only one [update cycle](../../../conceptual/table-update-model.md). A ring table retains those rows across update cycles.

## Syntax

```python syntax
ring_table(parent: Table, capacity: int, initialize: bool = True) -> Table
```

## Parameters

<ParamTable>
<Param name="parent" type="Table">

The parent table. The ring table ignores rows removed from the parent table. If the parent table modifies existing rows or shifts them to different row keys, the ring table fails with an error during that update cycle. Use a blink or append-only parent table.

</Param>
<Param name="capacity" type="int">

The maximum number of rows the ring table retains. When more rows are added to the parent table, the ring table drops the oldest rows so that only the latest `capacity` rows remain. `capacity` must be greater than 0.

</Param>
<Param name="initialize" type="bool">

Whether to initialize the ring table with a snapshot of the parent table. Default is `True`. If the parent table is static and `initialize` is `False`, `ring_table` raises a `DHError`.

</Param>
</ParamTable>

## Returns

An in-memory ring table. The ring table refreshes only if the parent table refreshes.

## Examples

The following example creates a source table with 5 rows, then creates a ring table with a capacity of 3. `result` holds only the last 3 rows of `source`.

```python order=source,result
from deephaven import new_table, ring_table
from deephaven.column import int_col

source = new_table([int_col("IntegerColumn", [1, 2, 3, 4, 5])])

result = ring_table(source, 3)
```

The following example creates a ring table with a capacity of 5 from a blink [time table](./timeTable.md). `source` holds only the rows added in the latest update cycle, while `result` holds up to the 5 most recent rows.

```python ticking-table order=null
from deephaven import time_table, ring_table

source = time_table("PT1S", blink_table=True)

result = ring_table(source, 5)
```

The following example creates two ring tables from a parent table that starts with 5 rows and then gains a new row every second. `result_initialized` uses the default `initialize=True`, so it starts with those 5 rows. `result_empty` sets `initialize=False`, so it starts empty and holds only rows added to `source` afterward.

```python ticking-table order=null
from deephaven import empty_table, time_table, ring_table, merge

static_source = empty_table(5).update(["X = i"])
dynamic_source = time_table("PT1S").update(["X = i + 5"]).drop_columns(["Timestamp"])
source = merge([static_source, dynamic_source])

result_initialized = ring_table(source, 10)
result_empty = ring_table(source, 10, initialize=False)
```

## Related documentation

- [Create a ring table](../../../how-to-guides/ring-table.md)
- [Create static tables](../../../how-to-guides/new-and-empty-table.md)
- [Table types](../../../conceptual/table-types.md#specialization-4-ring)
- [Incremental update model](../../../conceptual/table-update-model.md)
- [`empty_table`](./emptyTable.md)
- [`int_col`](./intCol.md)
- [`merge`](../merge/merge.md)
- [`new_table`](./newTable.md)
- [`time_table`](./timeTable.md)
- [Pydoc](/core/pydoc/code/deephaven.html#deephaven.ring_table)
