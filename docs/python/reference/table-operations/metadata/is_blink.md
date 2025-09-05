---
title: is_blink
---

The `is_blink` attribute returns a boolean value that is `True` if the table is a blink table, or `False` if it is not.

## Syntax

```python syntax
source.is_blink -> bool
```

## Parameters

This method takes no arguments.

## Returns

A boolean value that is `True` if the table is a blink table or `False` if it is not.

## Example

In this example we create two time tables, one with `blink_table` set to `True` and the other to `False`. Next, we call `blink_table` twice to confirm that the results are what we expect.

```python order=:log
from deephaven import time_table

tt1 = time_table(period="PT1S", blink_table=True)

tt2 = time_table(period="PT1S", blink_table=False)

print(tt1.is_blink)
print(tt2.is_blink)
```

## Related documentation

- [Pydoc](/core/pydoc/code/deephaven.table.html#deephaven.table.Table.is_blink)
