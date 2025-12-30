---
title: is_refreshing
---

The `is_refreshing` method returns a boolean value that is `True` if the table is refreshing, or `False` if it is not.

## Syntax

```python syntax
source.is_refreshing -> bool
```

## Parameters

This method takes no arguments.

## Returns

A boolean value that is `True` if the table is refreshing or `False` if it is not.

## Example

In this example, we create two tables - a static table and a time table - and check whether they are refreshing.

```python order=:log
from deephaven import time_table, new_table
from deephaven.column import string_col

t1 = new_table(
    [
        string_col("Title", ["content"]),
        string_col("ColumnName", ["column_content"]),
        string_col("AnotherColumn", ["string"]),
    ]
)

t2 = time_table("PT1S")

print(t1.is_refreshing)
print(t2.is_refreshing)
```

## Related documentation

- [Create a new table](../../../how-to-guides/new-and-empty-table.md#new_table)
- [Pydoc](/core/pydoc/code/deephaven.table.html#deephaven.table.Table.is_refreshing)
