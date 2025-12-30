---
title: median_by
---

`median_by` returns the median value for each group. Null values are ignored.

## Syntax

```
table.median_by(by: Union[str, list[str]]) -> Table
```

## Parameters

<ParamTable>
<Param name="by" type="Union[str, list[str]]" optional>

The column(s) by which to group data.

- `[]` returns the median value for each column in the table (default).
- `["X"]` will output the median value of each group in column `X`.
- `["X", "Y"]` will output the median value of each group designated from the `X` and `Y` columns.

</Param>
</ParamTable>

## Returns

A new table containing the median value for each group.

## Examples

In this example, `median_by` returns the median value for each column.

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

result = source.median_by()
```

In this example, `median_by` returns the median value, as grouped by `X`.

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

result = source.median_by(by=["X"])
```

In this example, `medianBy` returns the median value, as grouped by `X` and `Y`.

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

result = source.median_by(by=["X", "Y"])
```

## Related documentation

- [How to create static tables](../../../how-to-guides/new-and-empty-table.md)
- [How to perform dedicated aggregations](../../../how-to-guides/dedicated-aggregations.md)
- [`agg_by`](./aggBy.md)
- [`agg.median`](./AggMed.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/api/TableOperations.html#medianBy(java.lang.String...))
- [Pydoc](/core/pydoc/code/deephaven.table.html#deephaven.table.Table.median_by)
