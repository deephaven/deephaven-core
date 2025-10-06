---
title: count_by
---

`count_by` returns the number of rows for each group.

## Syntax

```
table.count_by(col: str, by: Union[str, list[str]]) -> Table
```

## Parameters

<ParamTable>
<Param name="col" type="str">

The name of the output column containing the count.

</Param>
<Param name="by" type="Union[str, list[str]]" optional>

The column(s) by which to group data.

- `[]` returns the total count of rows in the table (default).
- `["X"]` will output the count of each group in column `X`.
- `["X", "Y"]` will output the count of each group designated from the `X` and `Y` columns.

</Param>
</ParamTable>

## Returns

A new table containing the number of rows for each group.

## Examples

In this example, `count_by` returns the number of rows in the table and stores that in a new column, `Count`.

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

result = source.count_by("Count")
```

In this example, `count_by` returns the number of rows in the table as grouped by `X` and stores that in a new column, `Count`.

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

result = source.count_by("Count", by=["X"])
```

In this example, `count_by` returns the number of rows in the table as grouped by `X` and `Y`, and stores that in a new column `Count`.

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

result = source.count_by("Count", by=["X", "Y"])
```

## Related documentation

- [How to create static tables](../../../how-to-guides/new-and-empty-table.md)
- [How to perform dedicated aggregations](../../../how-to-guides/dedicated-aggregations.md)
- [`agg_by`](./aggBy.md)
- [`agg.count`](./AggCount.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/api/TableOperations.html#countBy(java.lang.String))
- [Pydoc](/core/pydoc/code/deephaven.table.html#deephaven.table.Table.count_by)
