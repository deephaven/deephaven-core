---
title: to_string
---

The `to_string` method returns the first _n_ rows of a table as a pipe-delimited string.

## Syntax

```python syntax
source.to_string(num_rows: int = 10, cols: Union[str, Sequence[str]] = None) -> str
```

## Parameters

<ParamTable>
<Param name="num_rows" type="int">

The number of rows from the beginning of the table to return. Default is 10.

</Param>
<Param name="cols" type="Union[str, Sequence[str]]" optional>

The columns name(s) to include in the string. Default is `None`, which includes all columns.

</Param>
</ParamTable>

## Returns

A pipe-delimited string containing the first _n_ rows of the table.

## Example

```python order=:log
from deephaven import new_table
from deephaven.column import string_col, int_col

source = new_table(
    [
        string_col(
            "Letter", ["A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L"]
        ),
        int_col("Numbers2", [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]),
    ]
)

# no args - default behavior
print(source.to_string())

# with args
print(source.to_string(4, "Letter"))
```

## Related documentation

- [Create a new table](../../../how-to-guides/new-and-empty-table.md#new_table)
- [Pydoc](/core/pydoc/code/deephaven.table.html#deephaven.table.Table.to_string)
