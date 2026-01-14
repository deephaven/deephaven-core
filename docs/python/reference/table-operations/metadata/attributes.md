---
title: attributes
---

The `attributes` method returns all of a Table's defined attributes.

## Syntax

```python syntax
source.attributes() -> dict
```

## Parameters

This method takes no arguments.

## Returns

A `Dict` of all of the Table's defined attributes.

## Example

Here, we create a table and a time table, and print their attributes.

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

print(source.attributes())
print(tt.attributes())
```

## Related documentation

- [Create a new table](../../../how-to-guides/new-and-empty-table.md#new_table)
- [Pydoc](/core/pydoc/code/deephaven.table.html#deephaven.table.Table.attributes)
