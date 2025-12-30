---
title: where_not_in
---

The `where_not_in` method returns a new table containing rows from the source table, where the rows **do not** match values in the filter table. The filter is updated whenever either table changes.

> [!NOTE]
> `where_not_in` is not appropriate for all situations. Its purpose is to enable more efficient filtering for an infrequently changing filter table.

## Syntax

```python syntax
table.where_not_in(filter_table: Table, cols: Union[str, list[str]]) -> Table
```

## Parameters

<ParamTable>
<Param name="filter_table" type="Table">

The table containing the set of values to filter on.

</Param>
<Param name="cols" type="Union[str, list[str]]">

A list of the columns (as [Strings](../../query-language/types/strings.md)) to match between the two tables.

- `"X"` will match on the same column name.
- `"X = Y"` will match when the columns have different names, with `X` being the source table column and `Y` being the filter table column.

Matches are defined the same as the set exclusion operator ([`not in`](../../query-language/match-filters/not-in.md)).

</Param>
</ParamTable>

## Returns

A new table containing rows from the source table, where the rows **do not** match values in the filter table.

## Examples

The following example creates a table containing only the colors not present in the `filter` table.

```python order=source,filter,result
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


filter = new_table([string_col("Colors", ["blue", "red", "purple", "white"])])

result = source.where_not_in(filter_table=filter, cols=["Color = Colors"])
```

The following example creates a table containing only the colors and codes present in the `filter` table. When using multiple matches, the resulting table will include only values that are in _both_ matches. In this example, only one row matches both color AND codes. This results in a new table that has one matching value.

```python order=source,filter,result
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
        int_col("Code", [12, 13, 11, 10, 16, 14, NULL_INT]),
    ]
)

filter = new_table(
    [
        string_col("Colors", ["blue", "red", "purple", "white"]),
        int_col("Codes", [10, 12, 14, 16]),
    ]
)

result = source.where_not_in(
    filter_table=filter, cols=["Color = Colors", "Code = Codes"]
)
```

## Related documentation

- [How to create static tables](../../../how-to-guides/new-and-empty-table.md)
- [How to use filters](../../../how-to-guides/use-filters.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/api/TableOperations.html#whereNotIn(TABLE,java.lang.String...))
- [Pydoc](/core/pydoc/code/deephaven.table.html#deephaven.table.Table.where_not_in)
