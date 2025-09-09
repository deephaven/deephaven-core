---
title: sum_by
---

`sum_by` returns the total sum for each group. Null values are ignored.

> [!CAUTION]
> Applying this aggregation to a column where the sum can not be computed will result in an error. For example, the sum is not defined for a column of string values.

## Syntax

```
table.sum_by(by: Union[str, list[str]]) -> Table
```

## Parameters

<ParamTable>
<Param name="by" type="Union[str, list[str]]" optional>

The column(s) by which to group data.

- `[]` returns the total sum for all non-key columns (default).
- `["X"]` will output the total sum of each group in column `X`.
- `["X", "Y"]` will output the total sum of each group designated from the `X` and `Y` columns.

</Param>
</ParamTable>

## Returns

A new table containing the sum for each group.

## Examples

In this example, `sum_by` returns the sum of the whole table. Because a sum can not be computed for the string columns `X` and `Y`, these columns are dropped before applying `sum_by`.

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

result = source.drop_columns(cols=["X", "Y"]).sum_by()
```

In this example, `sum_by` returns the sum, as grouped by `X`. Because a sum can not be computed for the string column `Y`, this column is dropped before applying `sum_by`.

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

result = source.drop_columns(cols=["Y"]).sum_by(by=["X"])
```

In this example, `sum_by` returns the sum, as grouped by `X` and `Y`.

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

result = source.sum_by(by=["X", "Y"])
```

## Related documentation

- [How to create static tables](../../../how-to-guides/new-and-empty-table.md)
- [How to perform dedicated aggregations](../../../how-to-guides/dedicated-aggregations.md)
- [`agg_by`](./aggBy.md)
- [`agg.sum`](./AggSum.md)
- [`drop_columns`](../select/drop-columns.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/api/TableOperations.html#sumBy(java.lang.String...))
- [Pydoc](/core/pydoc/code/deephaven.table.html#deephaven.table.Table.sum_by)
