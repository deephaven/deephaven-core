---
title: where
---

The `where` method filters rows of data from the source table.

> [!NOTE]
> The engine does not guarantee it evaluates filters in argument order. Within a run of consecutive stateless filters (the default), when the data source supports pushdown for a filter, the engine estimates its cost and can run a cheaper filter before one that appears earlier in the argument list. Within that run, filters without pushdown support run after the pushdown-capable ones, and filters with equal estimated cost keep their relative argument order. A stateful filter, such as one marked serial, is a boundary that this reordering never crosses. It stays in its argument position relative to its neighbors, though the stateless filters after it can still be reordered among themselves. It is still _best practice_ to place filters related to partitioning and grouping columns first, as significant data volumes can then be excluded, and match filters are highly optimized, so they should usually come before conditional filters. If your query depends on filters running in a specific order, use [`with_serial`](../../query-language/types/Filter.md#with_serial) or barriers to guarantee it.

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

The following shows how to apply a custom function as a filter. Take note that the function call must be explicitly cast to a `(boolean)` — this is required whenever the engine cannot statically determine the function's return type, which is the case here because `my_filter` has no return type hint. A function annotated `-> bool` does not need the cast.

```python order=source,result_filtered,result_not_filtered
from deephaven import new_table
from deephaven.column import int_col


def my_filter(int_):
    return int_ <= 4


source = new_table([int_col("IntegerColumn", [1, 2, 3, 4, 5, 6, 7, 8])])

result_filtered = source.where(filters=["(boolean)my_filter(IntegerColumn)"])
result_not_filtered = source.where(filters=["!((boolean)my_filter(IntegerColumn))"])
```

## Serial execution

By default, Deephaven can parallelize filter evaluation across multiple CPU cores when the input is large enough. For filters with side effects or order dependencies, use [`with_serial`](../../query-language/types/Filter.md#with_serial) to force sequential processing.

This filter tracks how many rows it evaluates. Once more than about 131,072 rows reach this filter, it becomes eligible for parallel evaluation — it is not guaranteed to run in parallel, since that also depends on available worker threads and, for a Python-backed filter, a free-threaded Python build; a standard GIL-enabled build never invokes it concurrently. That is current behavior, not a guarantee. Only `with_serial` promises that the filter's rows are evaluated in row-set order, so use it to protect a filter like this regardless of build. The example below uses 100 rows for clarity.

```python order=source,result
from deephaven.filters import Filter
from deephaven import empty_table

rows_checked = 0


def check_value(x) -> bool:
    global rows_checked
    rows_checked += 1  # Side effect: modifies external state
    return x > 5


source = empty_table(100).update("X = i")

# Use with_serial because the filter has side effects
f = Filter.from_("check_value(X)").with_serial()
result = source.where(f)
```

See [Parallelization](../../../conceptual/query-engine/parallelization.md) for more details.

## Filters on partitioning columns

When a table comes from a partitioned source, such as a directory of Parquet files or an Iceberg table, a filter that uses only partitioning columns can be applied to the partitions before any data is read. Deephaven evaluates it once per partition instead of once per row, and runs it ahead of the other filters, so whole partitions are skipped.

Deephaven applies a filter this way even when filters are configured to be stateful by default, because that is nearly always what users want. For example, `Date = today()` is stateful when filters are stateful by default, but Deephaven still evaluates it early, partition by partition.

A filter on partitioning columns is not applied this way if:

- It is marked serial with [`with_serial`](../../query-language/types/Filter.md#with_serial), or any filter before it in the argument list is. From the first serial filter on, Deephaven evaluates that filter and every later one on the table's rows instead of on whole partitions. Later stateless filters can still be reordered among themselves by cost.
- It respects a barrier declared by a filter that isn't applied this way.
- It uses row variables such as `i` or `ii`, or its results can change over time (a refreshing filter).

Mark a filter on partitioning columns serial only when the order in which it's evaluated matters.

## Related documentation

- [Create a new table](../../../how-to-guides/new-and-empty-table.md)
- [How to use filters](../../../how-to-guides/use-filters.md)
- [Parallelization](../../../conceptual/query-engine/parallelization.md)
- [Filter](../../query-language/types/Filter.md)
- [equals](../../query-language/match-filters/equals.md)
- [`icase in`](../../query-language/match-filters/icase-in.md)
- [`icase not in`](../../query-language/match-filters/icase-not-in.md)
- [`in`](../../query-language/match-filters/in.md)
- [not equals (`!=`)](../../query-language/match-filters/not-equals.md)
- [`not in`](../../query-language/match-filters/not-in.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/api/TableOperations.html#where(java.lang.String...))
- [Pydoc](/core/pydoc/code/deephaven.table.html#deephaven.table.Table.where)
