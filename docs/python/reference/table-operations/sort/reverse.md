---
title: reverse
---

`reverse` reverses the order of rows in a table.

## Syntax

```
table.reverse() -> Table
```

## Parameters

## Returns

A new table with all of the rows from the input table in reverse order.

## Examples

```python order=source,result
from deephaven import new_table
from deephaven.column import string_col, int_col

source = new_table(
    [
        string_col(
            "Tool",
            [
                "Paring",
                "Bread",
                "Steak",
                "Butter",
                "Teaspoon",
                "Spoon",
                "Slotted",
                "Ladle",
            ],
        ),
        string_col(
            "Type",
            ["Knife", "Knife", "Knife", "Knife", "Spoon", "Spoon", "Spoon", "Spoon"],
        ),
        int_col("Quantity", [5, 8, 11, 10, 24, 3, 2, 4]),
    ]
)

result = source.reverse()
```

## Related documentation

- [Create a new table](../../../how-to-guides/new-and-empty-table.md#new_table)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/api/TableOperations.html#reverse())
- [Pydoc](/core/pydoc/code/deephaven.table.html#deephaven.table.Table.reverse)
