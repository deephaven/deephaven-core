---
title: where
---

The `where` method filters rows of data from the source table.

> [!NOTE]
> The engine will address filters in series, consistent with the ordering of arguments. It is _best practice_ to place filters related to partitioning and grouping columns first, as significant data volumes can then be excluded. Additionally, match filters are highly optimized and should usually come before conditional filters.

## Syntax

```python syntax
table.where(filters: Union[str, Filter, Sequence[str], Sequence[Filter]]) -> Table
```

## Parameters

<ParamTable>
<Param name="filters" type="Union[str, Filter, Sequence[str], Sequence[Filter]]">

Formulas for filtering as a list of [Strings](../../query-language/types/strings.md).

Any filter is permitted, as long as it is not refreshing and does not use row position/key variables or arrays.

</Param>
</ParamTable>

## Returns

A new table with only the rows meeting the filter criteria in the column(s) of the source table.

## Examples

The following example returns rows where `Color` is `blue`.

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


result = source.where(filters=["Color = `blue`"])
```

The following example returns rows where `Number` is greater than 3.

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

result = source.where(filters=["Number > 3"])
```

The following returns rows where `Color` is `blue` and `Number` is greater than 3.

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
result = source.where(filters=["Color = `blue`", "Number > 3"])
```

The following returns rows where `Color` is `blue` or `Number` is greater than 3.

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


result = source.where_one_of(filters=["Color = `blue`", "Number > 3"])
```

The following shows how to apply a custom function as a filter. Take note that the function call must be explicitly cast to a `(boolean)`.

```python order=source,result_filtered,result_not_filtered
from deephaven import new_table
from deephaven.column import int_col


def my_filter(int_):
    return int_ <= 4


source = new_table([int_col("IntegerColumn", [1, 2, 3, 4, 5, 6, 7, 8])])

result_filtered = source.where(filters=["(boolean)my_filter(IntegerColumn)"])
result_not_filtered = source.where(filters=["!((boolean)my_filter(IntegerColumn))"])
```

## Related documentation

- [How to create static tables](../../../how-to-guides/new-and-empty-table.md)
- [How to use filters](../../../how-to-guides/use-filters.md)
- [equals](../../query-language/match-filters/equals.md)
- [not equals (`!=`)](../../query-language/match-filters/not-equals.md)
- [`icase in`](../..//query-language/match-filters/icase-in.md)
- [`icase not in`](../../query-language/match-filters/icase-not-in.md)
- [`in`](../../query-language/match-filters/in.md)
- [`not in`](../../query-language/match-filters/not-in.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/api/TableOperations.html#where(java.lang.String...))
- [Pydoc](/core/pydoc/code/deephaven.table.html#deephaven.table.Table.where)
