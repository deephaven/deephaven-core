---
title: where_one_of
---

The `where_one_of` method returns a new table containing rows from the source table, where the rows match at least one filter.

## Syntax

```python syntax
where_one_of(filters: Union[str, Filter, Sequence[str], Sequence[Filter]]) -> Table
```

## Parameters

<ParamTable>
<Param name="filters" type="Union[str, Filter, Sequence[str], Sequence[Filter]]">

The filter condition expressions.

</Param>
</ParamTable>

## Returns

A new table containing rows from the source table, where the rows match at least one filter.

## Examples

The following example creates a table containing only the colors not present in the `filter` table.

```python order=source,result
from deephaven import new_table
from deephaven.column import string_col, int_col, double_col
from deephaven.constants import NULL_INT

source = new_table(
    [
        string_col("Letter", ["A", "C", "F", "B", "E", "D", "A"]),
        int_col("Number", [NULL_INT, 2, 1, NULL_INT, 4, 5, 3]),
        string_col(
            "Color", ["red", "blue", "orange", "purple", "yellow", "pink", "blue"]
        ),
        int_col("Code", [12, 14, 11, NULL_INT, 16, 14, NULL_INT]),
    ]
)

result = source.where_one_of(filters=["Letter = 'A'", "Color = 'blue'"])
```

## Related documentation

- [How to create static tables](../../../how-to-guides/new-and-empty-table.md)
- [How to use filters](../../../how-to-guides/use-filters.md)
- [Pydoc](/core/pydoc/code/deephaven.table.html#deephaven.table.Table.where_one_of)
