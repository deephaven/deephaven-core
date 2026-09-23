---
title: Convert add- and append-only tables to blink tables
---

This guide shows you how to convert a refreshing add-only or append-only table to a blink table. It's a simple process that can be performed with a single method, [`add_only_to_blink`](../reference/table-operations/create/add-only-to-blink.md).

## Example

In this example, we create a time table that refreshes every second, then convert it to a blink table using [`add_only_to_blink`](../reference/table-operations/create/add-only-to-blink.md).

```python ticking-table order=null
from deephaven.stream import add_only_to_blink
from deephaven import time_table

# create source table
source = time_table("PT1S")

# convert `source` table to a blink table
result = add_only_to_blink(source)
```

![The `source` and `result` tables ticking side-by-side in the Deephaven console](../assets/how-to/add-only-to-blink.png)

## Related documentation

- [Create a time table](../how-to-guides/time-table.md)
