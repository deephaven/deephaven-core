---
title: sort
slug: ./sort
---

`sort` sorts rows in a table in a smallest to largest order based on the column(s) listed in the `columns_to_sort_by` argument.

The `sort` method can also accept `sort_column` pairs, which allow some columns to be sorted in ascending order while other columns are sorted in descending order.

## Syntax

```
sort(
    order_by: Union[str, Sequence[str]],
    order: Union[SortDirection, Sequence[SortDirection]] = None,
) -> Table
```

## Parameters

<ParamTable>
<Param name="order_by" type="Union[str, Sequence[str]]">

The column(s) to sort in ascending order.

</Param>
<Param name="order" type="Union[SortDirection, Sequence[SortDirection]]" optional>

A columns and directions to sort by. Options are `ASCENDING` and `DESCENDING`.

</Param>
</ParamTable>

## Returns

A new table where (1) rows are sorted in a smallest to largest order based on the column(s) listed in the `columns_to_sort_by` argument or (2) where rows are sorted in the order defined by the `sort_columns` listed in the `sort` argument.

## Examples

In the following example, `sort` will return a new table with the `Letter` column sorted in ascending order.

```python order=source,result
from deephaven import new_table
from deephaven.column import string_col, int_col

source = new_table(
    [
        string_col("Letter", ["A", "B", "A", "B", "B", "A"]),
        int_col("Number", [6, 6, 1, 3, 4, 4]),
        string_col("Color", ["red", "blue", "orange", "purple", "yellow", "pink"]),
    ]
)

result = source.sort(order_by=["Letter"])
```

In the following example, the `Number` column is first sorted in ascending order, _then_ the `Letter` column is sorted in ascending order.

```python order=source,result
from deephaven import new_table
from deephaven.column import string_col, int_col

source = new_table(
    [
        string_col("Letter", ["A", "B", "A", "B", "B", "A"]),
        int_col("Number", [6, 6, 1, 3, 4, 4]),
        string_col("Color", ["red", "blue", "orange", "purple", "yellow", "pink"]),
    ]
)

result = source.sort(order_by=["Number", "Letter"])
```

In the following example, `sort` accepts objects returned from `sort_columns`. The `Number` column is first sorted in ascending order, _then_ the `Letter` column is sorted in descending order.

```python order=source,result
from deephaven import new_table, SortDirection
from deephaven.column import string_col, int_col, double_col

source = new_table(
    [
        string_col("Letter", ["A", "B", "A", "B", "B", "A"]),
        int_col("Number", [6, 6, 1, 3, 4, 4]),
        string_col("Color", ["red", "blue", "orange", "purple", "yellow", "pink"]),
    ]
)

sort_columns = [SortDirection.ASCENDING, SortDirection.DESCENDING]

result = source.sort(order_by=["Number", "Letter"], order=sort_columns)
```

The following example returns the same table as the prior example. The `Number` column is first sorted in ascending order, _then_ the `Letter` column is sorted in descending order.

```python order=source,result
from deephaven import new_table
from deephaven.column import string_col, int_col, double_col

source = new_table(
    [
        string_col("Letter", ["A", "B", "A", "B", "B", "A"]),
        int_col("Number", [6, 6, 1, 3, 4, 4]),
        string_col("Color", ["red", "blue", "orange", "purple", "yellow", "pink"]),
    ]
)

result = source.sort_descending(order_by=["Letter"]).sort(order_by=["Number"])
```

## Related documentation

- [Create a new table](../../../how-to-guides/new-and-empty-table.md#new_table)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/api/TableOperations.html#sort(java.lang.String...))
- [Pydoc](/core/pydoc/code/deephaven.table.html#deephaven.table.Table.sort)
