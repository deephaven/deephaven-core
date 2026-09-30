---
title: where
---

The `where` method filters rows of data from the source table.

> [!NOTE]
> The engine does not guarantee it evaluates filters in argument order: when the data source supports pushdown for a stateless filter (the default), the engine estimates its cost and can run a cheaper filter before one that appears earlier in the argument list. Filters without pushdown support, and filters with equal estimated cost, keep their argument order. It is still _best practice_ to place filters related to partitioning and grouping columns first, as significant data volumes can then be excluded, and match filters are highly optimized, so they should usually come before conditional filters. If your query depends on filters running in a specific order, use [`withSerial`](../../query-language/types/Filter.md#withserial) or barriers to guarantee it.

## Syntax

```
table.where(filters...)
```

## Parameters

<ParamTable>
<Param name="filters" type="String...">

Formulas for filtering as a list of [Strings](../../query-language/types/strings.md).

</Param>
<Param name="filters" type="Collection">

Collection of formulas for filtering.

</Param>
<Param name="filter" type="Filter">

A [`Filter`](../../query-language/types/Filter.md) object, such as a serial filter or one that declares or respects barriers.

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

The following returns rows where `Color` is `blue` and `Number` is greater than 3.

```groovy order=source,result
source = newTable(
    stringCol("Letter", "A", "C", "F", "B", "E", "D", "A"),
    intCol("Number", NULL_INT, 2, 1, NULL_INT, 4, 5, 3),
    stringCol("Color", "red", "blue", "orange", "purple", "yellow", "pink", "blue"),
    intCol("Code", 12, 13, 11, NULL_INT, 16, 14, NULL_INT),
)

result = source.where("Color = `blue`", "Number > 3")
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

The following shows how to apply a custom function as a filter. Take note that the function call must be explicitly cast to a `(boolean)` — this is required because the query-language compiler can't determine a closure's return type, so it types the call as `Object`. A native method with a declared `boolean` return type does not need the cast.

```groovy order=source,result_filtered,result_not_filtered
my_filter = { int a -> a <= 4 }

source = newTable(
    intCol("IntegerColumn", 1, 2, 3, 4, 5, 6, 7, 8)
)

result_filtered = source.where("(boolean)my_filter(IntegerColumn)")
result_not_filtered = source.where("!((boolean)my_filter(IntegerColumn))")
```

## Serial execution

By default, Deephaven can parallelize filter evaluation across multiple CPU cores when the input is large enough. For filters with side effects or order dependencies, use [`withSerial`](../../query-language/types/Filter.md#withserial) to force sequential processing.

This filter tracks how many rows it evaluates. On a source with more than 131,072 rows, the filter becomes eligible for parallel evaluation — it is not guaranteed to run in parallel, since that also depends on available worker threads and a parallel-capable filter — and the counter could produce incorrect results if it does. The example below uses 100 rows for clarity; use `withSerial` to protect larger inputs:

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

When a table comes from a partitioned source, such as a directory of Parquet files or an Iceberg table, a filter that uses only partitioning columns can be applied to the partitions before any data is read. Deephaven evaluates it once per partition instead of once per row, and runs it ahead of the other filters, so whole partitions are skipped.

Deephaven applies a filter this way even when filters are configured to be stateful by default, because that is nearly always what users want. For example, `Date = today()` is stateful when filters are stateful by default, but Deephaven still evaluates it early, partition by partition.

A filter on partitioning columns is not applied this way if:

- It is marked serial with [`withSerial`](../../query-language/types/Filter.md#withserial), or any filter before it in the argument list is. From the first serial filter on, Deephaven evaluates that filter and every later one on the table's rows, in argument order.
- It respects a barrier declared by a filter that isn't applied this way.
- It uses row variables such as `i` or `ii`, or its results can change over time (a refreshing filter).

Mark a filter on partitioning columns serial only when the order in which it's evaluated matters.

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
