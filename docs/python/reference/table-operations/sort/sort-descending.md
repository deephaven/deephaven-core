---
title: sort_descending
---

`sort_descending` sorts rows in a table in a largest to smallest order based on the column(s) listed in the `order_by` argument.

## Syntax

```
sort_descending(order_by: Union[str, Sequence[str]]) -> Table
```

## Parameters

<ParamTable>
<Param name="order_by" type="Union[str, Sequence[str]]">

The column(s) used for sorting.

</Param>
</ParamTable>

## Returns

A new table where rows in a table are sorted in a largest to smallest order based on the column(s) listed in the `order_by` argument.

## Examples

```python order=source,result
from deephaven import new_table
from deephaven.column import string_col, int_col

source = new_table(
    [
        string_col("Letter", ["A", "C", "F", "B", "E", "D"]),
        int_col("Number", [6, 2, 1, 3, 4, 5]),
    ]
)

result = source.sort_descending(order_by=["Letter"])
```

```python order=source,result
from deephaven import new_table
from deephaven.column import string_col, int_col

source = new_table(
    [
        string_col("Letter", ["A", "B", "A", "B", "B", "A"]),
        int_col("Number", [6, 2, 1, 3, 4, 5]),
        string_col("Color", ["red", "blue", "orange", "purple", "yellow", "pink"]),
    ]
)

result = source.sort_descending(order_by=["Letter", "Number"])
```

## Related documentation

- [Create a new table](../../../how-to-guides/new-and-empty-table.md#new_table)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/api/TableOperations.html#sortDescending(java.lang.String...))
- [Pydoc](/core/pydoc/code/deephaven.table.html#deephaven.table.Table.sort_descending)
