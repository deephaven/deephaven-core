---
title: rename_columns
---

The `rename_columns` method creates a new table with specified columns renamed.

## Syntax

```
rename_columns(cols: Union[str, Sequence[str]]) -> Table
```

## Parameters

<ParamTable>
<Param name="cols" type="Union[str, Sequence[str]]">

Columns that will be renamed in the new table.

- `"X = Y"` will rename source column `Y` to `X`.

</Param>
</ParamTable>

> [!IMPORTANT]
> If the new column name conflicts with an existing column name in the table, the existing column is silently replaced.

## Returns

A new table that renames the specified columns.

## Examples

The following example renames columns `A` and `C`:

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

result = source.rename_columns(cols=["Fruit = A", "Type = C"])
```

The following example renames column `C` to `A`. Because `A` already exists, it is silently replaced with the renamed `C` column:

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

result = source.rename_columns(cols=["A = C"])
```

## Related documentation

- [Create a new table](../../../how-to-guides/new-and-empty-table.md#new_table)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/engine/table/Table.html#renameColumns(java.lang.String...))
- [Pydoc](/core/pydoc/code/deephaven.table.html#deephaven.table.Table.rename_columns)
