---
title: Convert add- and append-only tables to blink tables
---

This guide will show you how to convert a refreshing add-only or append-only table to a blink table. It's a simple process that can be performed with a single method, [`addOnlyToBlink`](../reference/table-operations/create/addOnlyToBlink.md).

## Example

In this example, we will create a time table that refreshes every second, then convert it to a blink table using [`addOnlyToBlink`](../reference/table-operations/create/addOnlyToBlink.md).

```groovy order=null
import io.deephaven.engine.table.impl.AddOnlyToBlinkTableAdapter

// create source table
source = timeTable("PT1S")

// convert `source` table to a blink table
result = AddOnlyToBlinkTableAdapter.toBlink(source)
```

![The `source` and `result` tables ticking side-by-side in the Deephaven console](../assets/how-to/add-only-to-blink.png)

## Related documentation

- [Create a time table](../how-to-guides/time-table.md)
