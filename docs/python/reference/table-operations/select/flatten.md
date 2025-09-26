---
title: flatten
---

The `flatten` method returns a new copy of the source table with a flat row set - i.e., from 0 to number of rows - 1.

## Syntax

```python syntax
table.flatten() -> Table
```

## Parameters

This method takes no arguments.

## Returns

A Table with a flat row set.

## Example

The following example creates a non-flat time table, and then flattens it.

```python order=tt,result
from deephaven.agg import first
from deephaven import time_table

tt = time_table("PT1S").update("X = ii").agg_by([first("X")], "Timestamp")

print(tt.is_flat)

result = tt.flatten()

print(result.is_flat)
```

## Related documentation

- [Pydoc](/core/pydoc/code/deephaven.table.html#deephaven.table.Table.flatten)
