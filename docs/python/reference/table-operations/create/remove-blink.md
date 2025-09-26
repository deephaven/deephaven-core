---
title: remove_blink
---

The `remove_blink` method returns a new non-blink copy of the source table or the source table itself if it is already a non-blink table. This is useful when you want to disable the default aggregation <!--TODO: link conceptual/table-types#specialized semantics when merged--> behavior of a blink table.

By default, many aggregations on blink tables are computed over _every row_ that the table has seen since the aggregation was called. Although data in a blink table disappears every update cycle, some aggregations can still be computed as though the whole data history were available.

## Syntax

```python syntax
table.remove_blink() -> Table
```

## Parameters

This method takes no arguments.

## Returns

A new non-blink Table.

## Example

The following example creates a simple blink table and then calls `remove_blink` to make it a non-blink table. We then demonstrate the difference in aggregation behavior between a blink table and a non-blink table by summing the `X` column in each table.

```python order=t_no_blink,t_blink
from deephaven import time_table

t_blink = time_table("PT1s", blink_table=True)
t_no_blink = t_blink.remove_blink()
```

## Related documentation

- [`time_table`](./timeTable.md)
