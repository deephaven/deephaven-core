---
title: group_by
---

`group_by` groups column content into vectors.

## Syntax

```
table.group_by(by: Union[str, list[str]]) -> Table
```

## Parameters

<ParamTable>
<Param name="by" type="Union[str, list[str]]" optional>

The column(s) by which to group data.

- `[]` the content of each column is grouped into its own vector (default).
- `["X"]` will output an array of values for each group in column `X`.
- `["X", "Y"]` will output a vector of values for each group designated from the `X` and `Y` columns.

</Param>
</ParamTable>

## Returns

A new table containing grouping columns and grouped data. Column content is grouped into vectors.

## Examples

In this example, `group_by` creates a vector of values for each column.

```python order=source,result
from deephaven import new_table
from deephaven.column import string_col, int_col

source = new_table(
    [
        string_col("X", ["A", "B", "A", "C", "B", "A", "B", "B", "C"]),
        string_col("Y", ["M", "N", "O", "N", "P", "M", "O", "P", "M"]),
        int_col("Number", [55, 76, 20, 130, 230, 50, 73, 137, 214]),
    ]
)

result = source.group_by()
```

In this example, `group_by` creates a vector of values, as grouped by `X`.

```python order=source,result
from deephaven import new_table
from deephaven.column import string_col, int_col

source = new_table(
    [
        string_col("X", ["A", "B", "A", "C", "B", "A", "B", "B", "C"]),
        string_col("Y", ["M", "N", "O", "N", "P", "M", "O", "P", "M"]),
        int_col("Number", [55, 76, 20, 130, 230, 50, 73, 137, 214]),
    ]
)

result = source.group_by(by=["X"])
```

In this example, `group_by` creates a vector of values, as grouped by `X` and `Y`.

```python order=source,result
from deephaven import new_table
from deephaven.column import string_col, int_col

source = new_table(
    [
        string_col("X", ["A", "B", "A", "C", "B", "A", "B", "B", "C"]),
        string_col("Y", ["M", "N", "O", "N", "P", "M", "O", "P", "M"]),
        int_col("Number", [55, 76, 20, 130, 230, 50, 73, 137, 214]),
    ]
)

result = source.group_by(by=["X", "Y"])
```

## Related documentation

- [How to create static tables](../../../how-to-guides/new-and-empty-table.md)
- [How to create multiple summary statistics for groups](../../../how-to-guides/combined-aggregations.md)
- [How to group and ungroup data](../../../how-to-guides/grouping-data.md)
- [`agg_by`](./aggBy.md)
- [`ungroup`](./ungroup.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/api/TableOperations.html#groupBy(java.lang.String...))
- [Pydoc](/core/pydoc/code/deephaven.table.html#deephaven.table.Table.group_by)
