---
title: where
---

The [`where`](#syntax) method filters rows of data from the source table.

> [!NOTE]
> The engine does not guarantee that it evaluates filters in argument order. See [Filter evaluation order](#filter-evaluation-order) for the order it uses and how to guarantee a specific one.

## Syntax

```python syntax
table.where(filters: Optional[Union[str, Filter, Sequence[str], Sequence[Filter]]] = None) -> Table
```

## Parameters

<ParamTable>
<Param name="filters" type="Optional[Union[str, Filter, Sequence[str], Sequence[Filter]]]" Optional>

Formulas for filtering as a list of [Strings](../../query-language/types/strings.md), or [`Filter`](../../query-language/types/Filter.md) objects, such as a serial filter or one that declares or respects [Barriers](../../query-language/types/Barrier.md).

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

The following returns rows where `Color` is `blue` and `Number` is greater than 2.

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
result = source.where(filters=["Color = `blue`", "Number > 2"])
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

The following shows how to apply a custom function as a filter. The function call must be explicitly cast to a `(boolean)`. This is required whenever the engine cannot statically determine the function's return type, which is the case here because `my_filter` has no return type hint. A function annotated `-> bool` does not need the cast.

```python order=source,result_filtered,result_not_filtered
from deephaven import new_table
from deephaven.column import int_col


def my_filter(int_):
    return int_ <= 4


source = new_table([int_col("IntegerColumn", [1, 2, 3, 4, 5, 6, 7, 8])])

result_filtered = source.where(filters=["(boolean)my_filter(IntegerColumn)"])
result_not_filtered = source.where(filters=["!((boolean)my_filter(IntegerColumn))"])
```

## Filter evaluation order

When [`where`](#syntax) receives several filters, Deephaven evaluates them one after another, and each filter evaluates only the rows that passed the filters before it. Deephaven parallelizes within a single filter by splitting that filter's rows across threads. It does not run different filters at the same time.

The order of the filters depends on whether each one is stateless or stateful:

- A **stateless** filter is one that Deephaven assumes is safe to evaluate in parallel. Every conditional formula filter is stateless by default, as [Stateless by default](../../../conceptual/query-table-configuration.md#stateless-by-default) describes.
- A **stateful** filter is one that Deephaven does not assume is safe to evaluate in parallel. A filter marked serial with [`with_serial`](../../query-language/types/Filter.md#with_serial) is stateful, and every conditional formula filter is stateful when stateless by default is turned off. This setting does not affect a match filter that Deephaven evaluates directly, which stays stateless. On a non-free-threaded Python build, a conditional formula filter that calls a Python function or uses Python objects is also stateful.

Deephaven applies these rules:

1. A stateful filter keeps its position in the argument list. Reordering never crosses it.
2. Within a run of consecutive stateless filters, a filter that respects a [Barrier](../../query-language/types/Barrier.md) runs after the filter that declares it. Next, filters whose data source supports [predicate pushdown](../../../how-to-guides/predicate-pushdown.md) run in order of estimated cost, cheaper first, ahead of filters without that support. Filters that tie keep their relative argument order.

To guarantee a specific order, use [`with_serial`](../../query-language/types/Filter.md#with_serial) or [Barriers](../../query-language/types/Barrier.md).

It remains good practice to write filters on partitioning columns first, because they exclude large data volumes early. Write [match filters](../../../how-to-guides/use-filters.md#match-filters) before conditional filters, because match filters are highly optimized.

## Serial execution

By default, Deephaven parallelizes the evaluation of a single filter by splitting its rows across CPU cores. Mark a filter serial with [`with_serial`](../../query-language/types/Filter.md#with_serial) when it has side effects or depends on row order. Deephaven then evaluates that filter's rows one at a time in row-set order, which is ascending row key.

Parallel evaluation applies only to large inputs. A filter becomes eligible when more than about 131,072 rows reach it, which counts the rows that survive earlier filters, not the size of the source table. [Parallel processing with `where`](../../../conceptual/query-table-configuration.md#parallel-processing-with-where) describes the threshold and how refreshing tables count rows. A filter that calls a Python function also needs a free-threaded Python build to run in parallel. On a non-free-threaded build, Deephaven never splits such a filter across cores.

Parallel eligibility is not an order guarantee. Only [`with_serial`](../../query-language/types/Filter.md#with_serial) promises row-set order.

The following example counts the rows a filter evaluates, which is a side effect, so the filter is marked serial. The example uses 100 rows for clarity.

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

A partitioned source, such as a directory of Parquet files or an Iceberg table, has partitioning columns whose values identify each partition. Deephaven can apply a filter that uses only partitioning columns to whole partitions before it reads any data. It evaluates the filter once per partition instead of once per row, and runs it ahead of the other filters, so whole partitions are skipped.

Deephaven applies such a filter this way even when [stateless by default](../../../conceptual/query-table-configuration.md#stateless-by-default) is turned off, because that is nearly always what users want. For example, with stateless by default turned off, `Date = today()` is stateful, but Deephaven still evaluates it early, partition by partition.

Deephaven does not apply a filter on partitioning columns this way in these cases:

- The filter is marked serial with [`with_serial`](../../query-language/types/Filter.md#with_serial), or any filter before it in the argument list is. From the first serial filter on, Deephaven evaluates that filter and every later one on the table's rows instead of on whole partitions.
- The filter respects a [Barrier](../../query-language/types/Barrier.md) declared by a filter that is not applied this way.
- The filter uses row variables such as `i` or `ii`, or its results can change over time (a refreshing filter).
- The filter uses column arrays, even when the arrays come from partitioning columns.

In these cases Deephaven does not use that filter to skip partitions. It evaluates the filter on the rows of the partitions that other filters on partitioning columns leave in. On a large partitioned source, that can be much slower. Mark a filter on partitioning columns serial only when the order in which it is evaluated matters.

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
