---
title: columns
---

The `columns` method returns all of a Table's column definitions, as a list of `Column` objects.

## Syntax

```python syntax
source.columns -> list
```

## Parameters

This method takes no arguments.

## Returns

A `list` of all of the Table's column definitions.

## Example

Here, we create a table and a time table, and print their lists of column definitions with two calls to `table.columns`.

```python order=:log
from deephaven import new_table, time_table
from deephaven.column import string_col

source = new_table(
    [
        string_col("Title", ["content"]),
        string_col("ColumnName", ["column_content"]),
        string_col("AnotherColumn", ["string"]),
    ]
)

tt = time_table("PT1S")

print(source.columns)
print(tt.columns)
```

## Related documentation

- [Create a new table](../../../how-to-guides/new-and-empty-table.md#new_table)
- [Pydoc](/core/pydoc/code/deephaven.table.html#deephaven.table.Table.columns)
