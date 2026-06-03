---
title: Convert add-only and append-only tables to blink tables
sidebar_label: Convert add-only tables to blink tables
---

This guide will show you how to convert a refreshing add-only or append-only table to a blink table. It's a simple process that can be performed with a single method, [`add_only_to_blink`](../reference/table-operations/create/add-only-to-blink.md).

## Example

In this example, we will create a time table that refreshes every second, then convert it to a blink table using [`add_only_to_blink`](../reference/table-operations/create/add-only-to-blink.md).

```python order=result,source
from deephaven.stream import add_only_to_blink
from deephaven import time_table

# create source table
source = time_table("PT1S")

# convert `source` table to a blink table
result = add_only_to_blink(source)
```

## Related documentation

- [Create a time table](../how-to-guides/time-table.md)
