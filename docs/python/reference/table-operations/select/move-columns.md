---
title: move_columns
---

The `move_columns` method creates a new table with specified columns moved to a specific column index value.

## Syntax

```
move_columns(idx: int, cols: Union[str, Sequence[str]]) -> Table
```

## Parameters

<ParamTable>
<Param name="idx" type="int">

Column index where the specified columns will be moved in the new table. The index is zero-based, so column index number 2 would be the third column.

</Param>
<Param name="cols" type="Union[str, Sequence[str]]">

Columns to be moved.

</Param>
</ParamTable>

## Returns

A new table with specified columns moved to a specific column index value.

## Examples

The following example moves column `C` to the second position (1st index) in the new table.

```python order=source,result
from deephaven import new_table
from deephaven.column import string_col, int_col

source = new_table(
    [
        string_col("A", ["apple", "apple", "orange", "orange", "plum", "plum"]),
        int_col("B", [1, 1, 2, 2, 3, 3]),
        string_col(
            "C", ["Macoun", "Opal", "Navel", "Cara Cara ", "Greengage", "Mirabelle"]
        ),
        int_col("D", [1, 2, 12, 3, 2, 3]),
    ]
)

result = source.move_columns(idx=1, cols=["C"])
```

The following example moves columns `B` and `C` to the second position (1st index) and third position in the new table.

```python order=source,result
from deephaven import new_table
from deephaven.column import string_col, int_col

source = new_table(
    [
        string_col("A", ["apple", "apple", "orange", "orange", "plum", "plum"]),
        int_col("B", [1, 1, 2, 2, 3, 3]),
        string_col(
            "C", ["Macoun", "Opal", "Navel", "Cara Cara ", "Greengage", "Mirabelle"]
        ),
        int_col("D", [1, 2, 12, 3, 2, 3]),
    ]
)

result = source.move_columns(idx=1, cols=["C", "D"])
```

## Related documentation

- [Create a new table](../../../how-to-guides/new-and-empty-table.md#new_table)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/engine/table/Table.html#moveColumns(int,java.lang.String...))
- [Pydoc](/core/pydoc/code/deephaven.table.html#deephaven.table.Table.move_columns)
