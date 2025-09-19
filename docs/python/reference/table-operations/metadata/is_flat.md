---
title: is_flat
---

The `is_flat` method returns a boolean value that is `True` if the table is flat, or `False` if it is not.

## Syntax

```python syntax
source.is_flat -> bool
```

## Parameters

This method takes no arguments.

## Returns

A boolean value that is `true` if the table is flat or `false` if it is not.

## Example

In this example we create a time table and a `result` table that has an aggregation. Next, we call `is_flat` twice to see which of the tables is flat.

```python order=:log
from deephaven.agg import first
from deephaven import time_table

tt = time_table("PT1S").update("X = ii")

result = tt.agg_by([first("X")], "Timestamp")

print(tt.is_flat)
print(result.is_flat)
```

## Related documentation

- [Create a new table](../../../how-to-guides/new-and-empty-table.md#new_table)
- [Pydoc](/core/pydoc/code/deephaven.table.html#deephaven.table.Table.is_flat)
