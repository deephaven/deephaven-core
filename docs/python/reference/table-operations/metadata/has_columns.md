---
title: has_columns
---

The `has_columns` method is used to check whether a table contains a column (or columns) with the provided column name(s).

## Syntax

```python syntax
source.has_columns(cols: Union[str, Sequence[str]])
```

## Parameters

<ParamTable>
<Param name="cols" type="Union[str, Sequence[str]]">

The column name(s).

</Param>
</ParamTable>

## Returns

A boolean value that is `True` if all columns are in the table, or `False` if _any_ of the provided column names are not found in the table.

## Example

The following example checks for the existence of certain column names in a table with three columns.

```python order=:log
from deephaven import new_table
from deephaven.column import string_col

source = new_table(
    [
        string_col("Title", ["content"]),
        string_col("ColumnName", ["column_content"]),
        string_col("AnotherColumn", ["string"]),
    ]
)

print(source.has_columns("Title"))
print(source.has_columns("NotAColumnName"))
print(source.has_columns(["Title", "ColumnName"]))
print(source.has_columns(["Title", "NotAName"]))
```

## Related documentation

- [Create a new table](../../../how-to-guides/new-and-empty-table.md#new_table)
- [Pydoc](/core/pydoc/code/deephaven.table.html#deephaven.table.Table.has_columns)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/engine/table/Table.html#hasColumns(java.lang.String...))
