---
title: where
---

The [`where`](#syntax) method filters rows of data from the source table.

> [!NOTE]
> The engine does not guarantee that it evaluates filters in argument order. See [Filter evaluation order](#filter-evaluation-order) for the order it uses and how to guarantee a specific one.

## Syntax

```
table.where(filters...)
```

## Parameters

<ParamTable>
<Param name="filters" type="String...">

Formulas for filtering as a list of [Strings](../../query-language/types/strings.md).

</Param>
<Param name="filter" type="Filter">

A [`Filter`](../../query-language/types/Filter.md) object, such as a serial filter or one that declares or respects [Barriers](../../query-language/types/Barrier.md).

</Param>
</ParamTable>

## Returns

A new table with only the rows meeting the filter criteria in the column(s) of the source table.

## Examples

The following example returns rows where `Color` is `blue`.

```groovy order=source,result
source = newTable(
    stringCol("Letter", "A", "C", "F", "B", "E", "D", "A"),
    intCol("Number", NULL_INT, 2, 1, NULL_INT, 4, 5, 3),
    stringCol("Color", "red", "blue", "orange", "purple", "yellow", "pink", "blue"),
    intCol("Code", 12, 13, 11, NULL_INT, 16, 14, NULL_INT),
)

result = source.where("Color = `blue`")
```

The following example returns rows where `Number` is greater than 3.

```groovy order=source,result
source = newTable(
    stringCol("Letter", "A", "C", "F", "B", "E", "D", "A"),
    intCol("Number", NULL_INT, 2, 1, NULL_INT, 4, 5, 3),
    stringCol("Color", "red", "blue", "orange", "purple", "yellow", "pink", "blue"),
    intCol("Code", 12, 13, 11, NULL_INT, 16, 14, NULL_INT),
)

result = source.where("Number > 3")
```

The following returns rows where `Color` is `blue` and `Number` is greater than 2.

```groovy order=source,result
source = newTable(
    stringCol("Letter", "A", "C", "F", "B", "E", "D", "A"),
    intCol("Number", NULL_INT, 2, 1, NULL_INT, 4, 5, 3),
    stringCol("Color", "red", "blue", "orange", "purple", "yellow", "pink", "blue"),
    intCol("Code", 12, 13, 11, NULL_INT, 16, 14, NULL_INT),
)

result = source.where("Color = `blue`", "Number > 2")
```

The following returns rows where `Color` is `blue` or `Number` is greater than 3.

```groovy order=source,result
import io.deephaven.api.filter.FilterOr
import io.deephaven.api.filter.Filter

source = newTable(
    stringCol("Letter", "A", "C", "F", "B", "E", "D", "A"),
    intCol("Number", NULL_INT, 2, 1, NULL_INT, 4, 5, 3),
    stringCol("Color", "red", "blue", "orange", "purple", "yellow", "pink", "blue"),
    intCol("Code", 12, 13, 11, NULL_INT, 16, 14, NULL_INT),
)

result = source.where(FilterOr.of(Filter.from("Color = `blue`", "Number > 3")))
```

The following shows how to apply a custom function as a filter. Take note that the function call must be explicitly cast to a `(boolean)` — this is required because the query-language compiler cannot determine a closure's return type, so it types the call as `Object`. A native method with a declared `boolean` return type does not need the cast.

```groovy order=source,result_filtered,result_not_filtered
my_filter = { int a -> a <= 4 }

source = newTable(
    intCol("IntegerColumn", 1, 2, 3, 4, 5, 6, 7, 8)
)

result_filtered = source.where("(boolean)my_filter(IntegerColumn)")
result_not_filtered = source.where("!((boolean)my_filter(IntegerColumn))")
```

## Filter evaluation order

When [`where`](#syntax) receives several filters, Deephaven evaluates them one after another, and each filter evaluates only the rows that passed the filters before it. Deephaven parallelizes within a single filter by splitting that filter's rows across threads. It does not run different filters at the same time.

The order of the filters depends on whether each one is stateless or stateful:

- A **stateless** filter is one that Deephaven assumes is safe to evaluate in parallel. Every string-formula filter is stateless by default, as [Stateless by default](../../../conceptual/query-table-configuration.md#stateless-by-default) describes.
- A **stateful** filter is one that Deephaven does not assume is safe to evaluate in parallel. A filter marked serial with [`withSerial`](../../query-language/types/Filter.md#withserial) is stateful, and every string-formula filter is stateful when stateless by default is turned off.

Deephaven applies these rules:

1. A stateful filter keeps its position in the argument list. Reordering never crosses it.
2. Within a run of consecutive stateless filters, a filter that respects a [Barrier](../../query-language/types/Barrier.md) runs after the filter that declares it. Next, filters whose data source supports [predicate pushdown](../../../how-to-guides/predicate-pushdown.md) run in order of estimated cost, cheaper first, ahead of filters without that support. Filters that tie keep their relative argument order.

To guarantee a specific order, use [`withSerial`](../../query-language/types/Filter.md#withserial) or [Barriers](../../query-language/types/Barrier.md).

It remains good practice to write filters on partitioning columns first, because they exclude large data volumes early. Write [match filters](../../../how-to-guides/filters.md#match-filters) before conditional filters, because match filters are highly optimized.

## Serial execution

By default, Deephaven parallelizes the evaluation of a single filter by splitting its rows across CPU cores. Mark a filter serial with [`withSerial`](../../query-language/types/Filter.md#withserial) when it has side effects or depends on row order. Deephaven then evaluates that filter's rows one at a time in row-set order, which is ascending row key.

Parallel evaluation applies only to large inputs. A filter becomes eligible when more than about 131,072 rows reach it, which counts the rows that survive earlier filters, not the size of the source table. [Parallel processing with `where`](../../../conceptual/query-table-configuration.md#parallel-processing-with-where) describes the threshold and how refreshing tables count rows.

Parallel eligibility is not an order guarantee. Only [`withSerial`](../../query-language/types/Filter.md#withserial) promises row-set order.

The following example counts the rows a filter evaluates, which is a side effect, so the filter is marked serial. The example uses 100 rows for clarity.

```groovy order=source,result
import io.deephaven.api.filter.Filter

rowsChecked = [0] as int[]

checkValue = { int x ->
    rowsChecked[0]++  // Side effect: modifies external state
    return x > 5
}

source = emptyTable(100).update("X = i")

// Use withSerial because the filter has side effects
// Filter.from() returns a collection; [0] gets the single filter
f = Filter.from("(boolean)checkValue(X)")[0].withSerial()
result = source.where(f)
```

See [Parallelization](../../../conceptual/query-engine/parallelization.md) for more details.

## Filters on partitioning columns

A partitioned source, such as a directory of Parquet files or an Iceberg table, has partitioning columns whose values identify each partition. Deephaven can apply a filter that uses only partitioning columns to whole partitions before it reads any data. It evaluates the filter once per partition instead of once per row, and runs it ahead of the other filters, so whole partitions are skipped.

Deephaven applies such a filter this way even when stateless by default is turned off, because that is nearly always what users want. For example, with stateless by default turned off, `Date = today()` is stateful, but Deephaven still evaluates it early, partition by partition.

Deephaven does not apply a filter on partitioning columns this way in these cases:

- The filter is marked serial with [`withSerial`](../../query-language/types/Filter.md#withserial), or any filter before it in the argument list is. From the first serial filter on, Deephaven evaluates that filter and every later one on the table's rows instead of on whole partitions.
- The filter respects a [Barrier](../../query-language/types/Barrier.md) declared by a filter that is not applied this way.
- The filter uses row variables such as `i` or `ii`, or its results can change over time (a refreshing filter).

In these cases Deephaven includes every partition in the table and evaluates the filter against every row. On a large partitioned source, that can be much slower. Mark a filter on partitioning columns serial only when the order in which it is evaluated matters.

## Related documentation

- [Create a new table](../../../how-to-guides/new-and-empty-table.md#newtable)
- [How to use filters](../../../how-to-guides/filters.md)
- [Parallelization](../../../conceptual/query-engine/parallelization.md)
- [Filter](../../query-language/types/Filter.md)
- [equals](../../query-language/match-filters/equals.md)
- [`icase in`](../../query-language/match-filters/icase-in.md)
- [`icase not in`](../../query-language/match-filters/icase-not-in.md)
- [`in`](../../query-language/match-filters/in.md)
- [not equals (`!=`)](../../query-language/match-filters/not-equals.md)
- [`not in`](../../query-language/match-filters/not-in.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/api/TableOperations.html#where(java.lang.String...))
