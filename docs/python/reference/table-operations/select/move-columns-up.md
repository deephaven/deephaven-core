---
title: move_columns_up
---

The `move_columns_up` method creates a new table with specified columns appearing first in order, to the far left.

## Syntax

```
move_columns_up(cols: Union[str, Sequence[str]]) -> Table
```

## Parameters

<ParamTable>
<Param name="cols" type="Union[str, Sequence[str]]">

Columns to move to the left side of the new table.

</Param>
</ParamTable>

## Returns

A new table with specified columns appearing first in order, to the far left.

## Examples

The following query moves column `B` to the first position in the resulting table.

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

result = source.move_columns_up(cols=["B"])
```

The following example moves columns `B` and `D` to the first positions in the resulting table.

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

result = source.move_columns_up(cols=["B", "D"])
```

## Related documentation

- [Create a new table](../../../how-to-guides/new-and-empty-table.md#new_table)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/engine/table/Table.html#moveColumnsUp(java.lang.String...))
- [Pydoc](/core/pydoc/code/deephaven.table.html#deephaven.table.Table.move_columns_up)
