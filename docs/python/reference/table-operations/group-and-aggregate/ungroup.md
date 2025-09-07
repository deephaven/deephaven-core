---
title: ungroup
---

`ungroup` ungroups column content. It is the opposite of the [`group_by`](./groupBy.md) method. `ungroup` expands columns containing either Deephaven arrays or Java arrays into columns of singular values.

> [!CAUTION]
> Arrays of different lengths within a row result in an error.

## Syntax

```
table.ungroup(cols: Union[str, list[str]]) -> Table
```

## Parameters

<ParamTable>
<Param name="cols" type="Union[str, list[str]]" optional>

The column(s) of data to ungroup.

- `[]` all array columns from the source table will be expanded (default).
- `["X"]` will expand column `X`.
- `["X", "Y"]` will expand columns `X` and `Y`.

</Param>
</ParamTable>

## Returns

A new table in which array columns from the source table are expanded into separate rows.

## Examples

In this example, `group_by` returns an array of values for each column, as grouped by `X`. `ungroup` will undo this operation and all array columns from the source table are expanded into separate rows.

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

array_table = source.group_by(by=["X"])

result = array_table.ungroup()
```

In this example, `group_by` returns an array of values for each column, as grouped by `X`, while `ungroup` will expand the created array `Y` so each element is a new row.

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

array_table = source.group_by(by=["X"])

result = array_table.ungroup(cols=["Y"])
```

## Related documentation

- [How to create static tables](../../../how-to-guides/new-and-empty-table.md)
- [How to group and ungroup data](../../../how-to-guides/grouping-data.md)
- [How to perform dedicated aggregations](../../../how-to-guides/dedicated-aggregations.md)
- [`group_by`](./groupBy.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/api/TableOperations.html#ungroup())
- [Pydoc](/core/pydoc/code/deephaven.table.html#deephaven.table.Table.ungroup)
