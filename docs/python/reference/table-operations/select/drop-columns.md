---
title: drop_columns
---

The `drop_columns` method creates a table with the same number of rows as the source table but omits any columns included in the arguments.

## Syntax

```
drop_columns(cols: Union[str, Sequence[str]]) -> Table
```

## Parameters

<ParamTable>
<Param name="cols" type="Union[str, Sequence[str]]">

Columns that will be omitted in the new table.

</Param>
</ParamTable>

## Returns

A new table that includes the same number of rows as the source table, with the specified column names omitted.

## Examples

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

result = source.drop_columns(cols=["B", "D"])
```

## Related documentation

- [Create a new table](../../../how-to-guides/new-and-empty-table.md#new_table)
- [`lazy_update`](./lazy-update.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/api/TableOperations.html#dropColumns(java.lang.String...))
- [Pydoc](/core/pydoc/code/deephaven.table.html#deephaven.table.Table.drop_columns)
