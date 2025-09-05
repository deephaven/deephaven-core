---
title: select_distinct
---

The `select_distinct` method creates a new table containing all of the unique values for a set of key columns.

When the `select_distinct` method is used on multiple columns, it looks for distinct sets of values in the selected columns.

## Syntax

```
select_distinct(formulas: Union[str, Sequence[str]]) -> Table
```

## Parameters

<ParamTable>
<Param name="formulas" type="Union[str, Sequence[str]]">

Key columns.

</Param>
</ParamTable>

## Returns

A new table containing all of the unique values for a set of key columns.

## Examples

```python order=source,result
from deephaven import new_table
from deephaven.column import string_col, int_col

source = new_table(
    [
        string_col("A", ["apple", "apple", "orange", "orange", "plum", "grape"]),
        int_col("B", [1, 1, 2, 2, 3, 3]),
    ]
)

result = source.select_distinct(formulas=["A"])
```

```python order=source,result
from deephaven import new_table
from deephaven.column import string_col, int_col

source = new_table(
    [
        string_col("A", ["apple", "apple", "orange", "orange", "plum", "grape"]),
        int_col("B", [1, 1, 2, 2, 3, 3]),
    ]
)

result = source.select_distinct(formulas=["A", "B"])
```

## Related documentation

- [Create a new table](../../../how-to-guides/new-and-empty-table.md#new_table)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/api/TableOperations.html#selectDistinct(java.lang.String...))
- [Pydoc](/core/pydoc/code/deephaven.table.html#deephaven.table.Table.select_distinct)
