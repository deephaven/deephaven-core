---
title: tail_by
---

`tail_by` returns the last `n` rows for each group.

## Syntax

```
table.tail_by(num_rows: int, by: Union[str, list[str]]) -> Table
```

## Parameters

<ParamTable>
<Param name="num_rows" type="int">

The number of rows to return for each group.

</Param>
<Param name="by" type="Union[str, list[str]]" optional>

The column(s) by which to group data.

- `["X"]` will output the entire last `n` row(s) of each group in column `X`.
- `["X", "Y"]` will output the entire last `n` row(s) of each group designated from the `X` and `Y` columns.

</Param>
</ParamTable>

## Returns

A new table containing the last `n` rows for each group.

## Examples

In this example, `tail_by` returns the last 2 rows, as grouped by `X`.

```python order=source,result
from deephaven import new_table
from deephaven.column import string_col, int_col

source = new_table(
    [
        string_col("X", ["A", "B", "A", "A", "B", "A", "B", "B", "B"]),
        string_col("Y", ["M", "M", "M", "N", "M", "M", "M", "M", "M"]),
        int_col("Number", [55, 76, 20, 130, 230, 50, 73, 137, 214]),
    ]
)

result = source.tail_by(2, by=["X"])
```

In this example, `tail_by` returns the last 2 rows, as grouped by `X` and `Y`.

```python order=source,result
from deephaven import new_table
from deephaven.column import string_col, int_col

source = new_table(
    [
        string_col("X", ["A", "B", "A", "A", "B", "A", "B", "B", "B"]),
        string_col("Y", ["M", "M", "M", "N", "M", "M", "M", "M", "M"]),
        int_col("Number", [55, 76, 20, 130, 230, 50, 73, 137, 214]),
    ]
)

result = source.tail_by(2, by=["X", "Y"])
```

## Related documentation

- [How to create static tables](../../../how-to-guides/new-and-empty-table.md)
- [How to perform dedicated aggregations](../../../how-to-guides/dedicated-aggregations.md)
- [`agg_by`](./aggBy.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/engine/table/Table.html#tailBy(long,java.lang.String...))
- [Pydoc](/core/pydoc/code/deephaven.table.html#deephaven.table.Table.tail_by)

<!--TODO: https://github.com/deephaven/deephaven-core/issues/778> -->
