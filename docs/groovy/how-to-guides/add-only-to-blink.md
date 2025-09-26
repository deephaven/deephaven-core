---
title: Convert add-only and append-only tables to blink tables
sidebar_label: Convert add-only tables to blink tables
---

This guide will show you how to convert a refreshing add-only or append-only table to a blink table. It's a simple process that can be performed with a single method, [`toBlink`](../reference/table-operations/create/toBlink.md).

## Example

In this example, we will create a time table that refreshes every second, then convert it to a blink table using [`toBlink`](../reference/table-operations/create/toBlink.md).

```groovy order=result,source
import io.deephaven.engine.table.impl.AddOnlyToBlinkTableAdapter

// create source table
source = timeTable("PT1S")

// convert `source` table to a blink table
result = AddOnlyToBlinkTableAdapter.toBlink(source)
```

## Related documentation

- [Create a time table](../how-to-guides/time-table.md)
