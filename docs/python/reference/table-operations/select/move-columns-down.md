---
title: move_columns_down
---

The `move_columns_down` method creates a new table with specified columns appearing last in order, to the far right.

## Syntax

```
move_columns_down(cols: Union[str, Sequence[str]]) -> Table
```

## Parameters

<ParamTable>
<Param name="cols" type="Union[str, Sequence[str]]">

Columns to move to the right side of the new table.

</Param>
</ParamTable>

## Returns

A new table with specified columns appearing last in order (furthest to the right).

## Examples

The following example moves column `B` to the last position in the resulting table.

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

result = source.move_columns_down(cols=["B"])
```

The following example moves columns `B` and `D` to the last positions in the resulting table.

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

result = source.move_columns_down(cols=["B", "D"])
```

## Related documentation

- [Create a new table](../../../how-to-guides/new-and-empty-table.md#new_table)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/engine/table/Table.html#moveColumnsDown(java.lang.String...))
- [Pydoc](/core/pydoc/code/deephaven.table.html#deephaven.table.Table.move_columns_down)
