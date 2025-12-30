---
title: size
---

The `size` method returns the current number of rows in the source table.

## Syntax

```python syntax
source.size -> int
```

## Parameters

This method takes no arguments.

## Returns

An `int` value representing the current number of rows in the source table.

## Example

```python order=:log
from deephaven import time_table, new_table
from deephaven.column import string_col

t1 = new_table([string_col("Title", ["content"])])

t2 = new_table([string_col("Title2", ["content", "string", "another string"])])

print(t1.size)
print(t2.size)
```

## Related documentation

- [Create a new table](../../../how-to-guides/new-and-empty-table.md#new_table)
- [Pydoc](/core/pydoc/code/deephaven.table.html#deephaven.table.Table.size)
