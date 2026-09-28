---
title: avg_by
---

`avg_by` returns the average (mean) of each non-key column for each group. Null values are ignored.

> [!CAUTION]
> Applying this aggregation to a column where the average cannot be computed will result in an error. For example, the average is not defined for a column of string values.

## Syntax

```
table.avg_by(by: Union[str, list[str]]) -> Table
```

## Parameters

<ParamTable>
<Param name="by" type="Union[str, list[str]]">

The column(s) by which to group data.

- `[]` returns total average for all non-key columns (default).
- `["X"]` will output the total average of each group in column `X`.
- `["X", "Y"]` will output the total average of each group designated from the `X` and `Y` columns.

</Param>
</ParamTable>

## Returns

A new table containing the average for each group.

## Examples

In this example, `avg_by` returns the average value for the table. Because an average cannot be computed for the string columns `X` and `Y`, these columns are dropped before applying `avg_by`.

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

result = source.drop_columns(cols=["X", "Y"]).avg_by()
```

In this example, `avg_by` returns the average value, as grouped by `X`. Because an average cannot be computed for the string column `Y`, this column is dropped before applying `avg_by`.

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

result = source.drop_columns(cols=["Y"]).avg_by(by=["X"])
```

In this example, `avg_by` returns the average value, as grouped by `X` and `Y`.

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

result = source.avg_by(by=["X", "Y"])
```

## Related documentation

- [How to create static tables](../../../how-to-guides/new-and-empty-table.md)
- [How to perform dedicated aggregations](../../../how-to-guides/dedicated-aggregations.md)
- [`agg.avg`](./AggAvg.md)
- [`agg_by`](./aggBy.md)
- [`drop_columns`](../select/drop-columns.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/api/TableOperations.html#avgBy(java.lang.String...))
- [Pydoc](/core/pydoc/code/deephaven.table.html#deephaven.table.Table.avg_by)
