---
title: where
---

The [`where`](#syntax) method filters rows of data from the source table.

> [!NOTE]
> The engine does not guarantee that it evaluates filters in argument order. See [Filter evaluation order](#control-filter-evaluation-order) for the order it uses and how to guarantee a specific one.

## Syntax

```
table.where(filters...)
table.where(filter)
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

The following shows how to apply a custom function as a filter. The function call must be explicitly cast to a `(boolean)`, because the query-language compiler cannot determine a closure's return type and so types the call as `Object`. A native method with a declared `boolean` return type does not need the cast.

```groovy order=source,result_filtered,result_not_filtered
my_filter = { int a -> a <= 4 }

source = newTable(
    intCol("IntegerColumn", 1, 2, 3, 4, 5, 6, 7, 8)
)

result_filtered = source.where("(boolean)my_filter(IntegerColumn)")
result_not_filtered = source.where("!((boolean)my_filter(IntegerColumn))")
```

## Control filter evaluation order

When [`where`](#syntax) receives several filters, Deephaven evaluates them one after another, and each filter evaluates only the rows that passed the filters before it. Deephaven parallelizes within a single filter by splitting that filter's rows across threads. It does not run different filters at the same time.

The order of the filters depends on whether each one is stateless or stateful:

- A **stateless** filter is one that Deephaven assumes is safe to evaluate in parallel. By default, a conditional formula filter is stateless. [Stateless by default](../../../conceptual/query-table-configuration.md#stateless-by-default) describes the setting.
- A **stateful** filter is one that Deephaven does not assume is safe to evaluate in parallel. These filters are stateful:
  - A filter marked serial with [`withSerial`](../../query-language/types/Filter.md#withserial).
  - A conditional formula filter, when [stateless by default](../../../conceptual/query-table-configuration.md#stateless-by-default) is turned off.

A [match filter](../../../how-to-guides/use-filters.md#match-filters) that Deephaven evaluates directly stays stateless regardless of the [stateless by default](../../../conceptual/query-table-configuration.md#stateless-by-default) setting.

Deephaven applies these rules:

1. A stateful filter keeps its position in the argument list. Reordering never crosses it. The exception is a filter on partitioning columns, which Deephaven can run first, as described under [Filters on partitioning columns](#filter-on-partitioning-columns).
2. Within a run of consecutive stateless filters, Deephaven orders the filters by these rules, in priority order:
   - A filter that respects a [Barrier](../../query-language/types/Barrier.md) runs after the filter that declares it.
   - Filters whose data source supports [predicate pushdown](../../../how-to-guides/predicate-pushdown.md) run in order of estimated cost, cheaper first, ahead of filters without that support.
   - Filters that tie keep their relative argument order.

To guarantee a specific order, use [`withSerial`](../../query-language/types/Filter.md#withserial) or [Barriers](../../query-language/types/Barrier.md).

When `where` runs on a partitioned source, Deephaven runs filters on partitioning columns first where it can, so their position in the argument list rarely matters. The section on partitioning columns below describes when it cannot. Listing [match filters](../../../how-to-guides/use-filters.md#match-filters) before conditional filters is worthwhile: a match filter that Deephaven evaluates directly does not compile or call a formula, and each later filter evaluates only the rows that earlier filters keep.

## Run a filter serially

By default, Deephaven parallelizes the evaluation of a single filter by splitting its rows across CPU cores. Mark a filter serial with [`withSerial`](../../query-language/types/Filter.md#withserial) when it has side effects or depends on row order. Deephaven then evaluates that filter's rows one at a time in row set order, which is ascending row key.

Parallel evaluation applies only to large inputs. A filter becomes eligible when more than about 131,072 rows reach it. The count includes only the rows that survive earlier filters, not the size of the source table. [Parallel processing with `where`](../../../conceptual/query-table-configuration.md#parallel-processing-with-where) describes the threshold and how refreshing tables count rows.

Parallel eligibility is not an order guarantee. Only [`withSerial`](../../query-language/types/Filter.md#withserial) promises row set order.

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

The serial filter evaluates each of the 100 rows exactly once, one at a time, so `rowsChecked[0]` is 100 after the `where` call.

See [Parallelization](../../../conceptual/query-engine/parallelization.md) for more details.

## Filter on partitioning columns

A partitioned source, such as a directory of [Parquet](../../../how-to-guides/data-import-export/parquet-import.md) files or an [Iceberg](../../../how-to-guides/data-import-export/iceberg.md) table, has partitioning columns whose values identify each partition. Deephaven can apply a filter that uses only partitioning columns to whole partitions before it reads any data. It evaluates the filter once per partition instead of once per row, and runs it ahead of the other filters, so whole partitions are skipped.

Deephaven applies such a filter this way even when [stateless by default](../../../conceptual/query-table-configuration.md#stateless-by-default) is turned off. For example, with stateless by default turned off, `Date = today()` is stateful, but Deephaven still evaluates it early, partition by partition.

Deephaven does not apply a filter on partitioning columns this way in these cases:

- The filter is marked serial with [`withSerial`](../../query-language/types/Filter.md#withserial), or any filter before it in the argument list is. From the first serial filter on, Deephaven evaluates that filter and every later one on the table's rows instead of on whole partitions.
- The filter respects a [Barrier](../../query-language/types/Barrier.md) declared by a filter that is not applied this way.
- The filter uses [row variables](../../query-language/variables/special-variables.md) such as `i`, `ii`, or `k`, or its results can change over time (a refreshing filter).
- The filter uses [column arrays](../../../conceptual/arrays-vs-vectors.md), even when the arrays come from partitioning columns.

In these cases Deephaven does not use that filter to skip partitions. It evaluates the filter on the rows of the partitions that other filters on partitioning columns leave in. On a large partitioned source, that can be much slower. Mark a filter on partitioning columns serial only when the order in which it is evaluated matters.

## Related documentation

- [Create a new table](../../../how-to-guides/new-and-empty-table.md#newtable)
- [Filter table data](../../../how-to-guides/use-filters.md)
- [Parallelization](../../../conceptual/query-engine/parallelization.md)
- [Filter](../../query-language/types/Filter.md)
- [`whereIn`](./where-in.md)
- [`whereNotIn`](./where-not-in.md)
- [equals](../../query-language/match-filters/equals.md)
- [`icase in`](../../query-language/match-filters/icase-in.md)
- [`icase not in`](../../query-language/match-filters/icase-not-in.md)
- [`in`](../../query-language/match-filters/in.md)
- [not equals (`!=`)](../../query-language/match-filters/not-equals.md)
- [`not in`](../../query-language/match-filters/not-in.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/api/TableOperations.html#where(java.lang.String...))
