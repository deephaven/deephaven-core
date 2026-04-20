---
title: Filters
---

This guide focuses on using filters within [query strings](./query-string-overview.md). Unlike [formulas](./formulas.md), which add new columns to a table, filters are boolean expressions used to create a new table that is a subset of an existing table. Filters are employed in the following table operations:

- [`where`](../reference/table-operations/filter/where.md)
- [`whereIn`](../reference/table-operations/filter/where-in.md)
- [`whereNotIn`](../reference/table-operations/filter/where-not-in.md)

Additionally, filters can be used in [partitioned table](./partitioned-tables.md) operations.

## What is a filter?

A filter is a query string expression that evaluates to `true` or `false`. The filter expression is applied to every row in a table and can be used to create a new table that is a subset of an existing table.

Filters can be applied to columns of any data type. For instance, numeric filters can keep only rows that match a specific value, are in a range, or are even/odd:

```groovy order=source,resultEq,resultRange,resultEven,resultWhereMultiple
source = emptyTable(10).update("X = ii", "Y = randomDouble(-1, 1)")
resultEq = source.where("X == 5")
resultRange = source.where("X >= 2 && X < 6")
resultEven = source.where("X % 2 == 0")
resultWhereMultiple = source.where("X <= 5", "Y >= 0")
```

String filtering provides significant flexibility. You can search for full string matches, substring matches, use regex, and more. The following example shows several string filters:

```groovy order=source,resultFullMatch,resultSubstringMatch,resultRegex
randName = {
  def names = ["Alex", "Bethany", "Yvette", "Samantha", "Ned"]
  return names[new Random().nextInt(names.size())]
}

source = emptyTable(10).update("Name = (String)randName()")
resultFullMatch = source.where("Name == `Alex`")
resultSubstringMatch = source.where("Name.contains(`an`)")
resultRegex = source.where("Name.matches(`^[AY].*`)")
```

## Types of filters

### Match filters

A match filter evaluates to true if the column value matches one specified in the filter.

Match filters can search for a single-value match:

```groovy order=source,resultMatch,resultNotMatch
source = emptyTable(10).update("X = ii")
resultMatch = source.where("X == 5")
resultNotMatch = source.where("X != 5")
```

Match filters can be applied to a column of any data type:

```groovy order=source,resultStringMatch,resultBooleanMatch
randName = {
    def names = ["Alex", "Bethany", "Yvette", "Samantha", "Ned"]
    return names[new Random().nextInt(names.size())]
}

source = emptyTable(10).update("Name = (String)randName()", "BooleanCol = randomBool()")
resultStringMatch = source.where("Name == `Alex`")
resultBooleanMatch = source.where("BooleanCol == true")
```

Match filters can also search for multiple values. This applies to columns of any data type. Deephaven offers some special match filters to do just this:

- `in`: Returns only rows where the column matches one of the specified values.
- `not in`: Returns only rows where the column does not match any of the specified values.

```groovy order=source,resultIn,resultNotIn
source = emptyTable(10).update("X = ii")
resultIn = source.where("X in 2,4,6")
resultNotIn = source.where("X not in 2,4,6")
```

> [!NOTE]
> Match filters created using the equality operators (`=`, `==` or `!=`) follow standard IEEE 754 rules for handling `NaN` values. Any comparison involving `NaN` returns `false`, except for `!=`, which returns `true` for all values.
>
> In contrast, match filters created with set inclusion syntax (`in`, `not in`) _will_ match `NaN` values. For example: `value in NaN, 10.0` will return `true` if `value` is `NaN` or `10.0`. Alternatively, you can use the `isNaN(value)` function to explicitly test for NaN values such as `isNaN(value) || value < 10.0`.

### Range filters

Range filters evaluate to true if the column value is within a specified range. This type of filter is typically applied to numeric columns but can be applied to any column that supports comparison operators.

```groovy order=source,resultGreaterThan,resultLessThan,resultRange,resultInRange
source = emptyTable(10).update("X = ii")
resultGreaterThan = source.where("X > 5")
resultLessThan = source.where("X < 5")
resultRange = source.where("X >= 2 && X < 6")
resultInRange = source.where("inRange(X, 2, 6)")
```

> [!NOTE]
> Null values are considered less than any non-null value for sorting and comparison purposes. Therefore, `<` and `<=` comparisons will always include `null`. To prevent this behavior, you can add an explicit null check; for example: `!isNull(value) && value < 10`.

> [!NOTE]
> Comparison operators on floating-point values follow standard IEEE 754 rules for handling `NaN` values. Any comparison involving `NaN` returns `false`, except for `!=`, which returns `true` for all values. To include `NaN` values in your comparisons, use the `isNaN(value)` function to explicitly test for NaN values, such as `isNaN(value) || value < 10.0`.

Both `resultRange` and `resultInRange` can instead be implemented by [conjunctively](#conjunctive) combining two separate range filters:

```groovy order=source,resultRangeConjunctive
source = emptyTable(10).update("X = ii")
resultRangeConjunctive = source.where("X >= 2", "X < 6")
```

You can also filter for data that is not in a range by using the `!` operator or by [disjunctively](#disjunctive) combining two separate range filters:

```groovy order=source,resultNotInRangeDisjunctive,resultNotInRangeFilterOr,resultNotInRange
import io.deephaven.api.filter.Filter

source = emptyTable(10).update("X = ii")
resultNotInRangeDisjunctive = source.where("X < 2 || X > 6")
resultNotInRangeFilterOr = source.where(Filter.or(Filter.from("X < 2", "X > 6")))
resultNotInRange = source.where("!inRange(X, 2, 6)")
```

### String filters

String filters return only rows that match the specified criteria for [string](./work-with-strings.md) columns. As such, string filters can use any [`java.lang.String`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/lang/String.html) methods.

The following example shows several string filters:

```groovy order=source,resultStringMatch,resultSubstringMatch,resultRegexMatch
randName = {
    def names = ["Alex", "Bethany", "Yvette", "Samantha", "Ned"]
    return names[new Random().nextInt(names.size())]
}

source = emptyTable(10).update("Name = (String)randName()", "BooleanCol = randomBool()")
resultStringMatch = source.where("Name == `Alex`")
resultSubstringMatch = source.where("Name.contains(`an`)")
resultRegexMatch = source.where("Name.matches(`^[AY].*`)")
```

Deephaven offers some additional special string match filters:

- `in`: Returns only rows where the string column matches one of the specified values.
- `icase in`: Returns only rows where the string column matches one of the specified values, ignoring case.
- `not in`: Returns only rows where the string column does not match any of the specified values.
- `icase not in`: Returns only rows where the string column does not match any of the specified values, ignoring case.
- `includes any`: Returns only rows where the string column contains any of the specified substrings.
- `includes all`: Returns only rows where the string column contains all of the specified substrings.

```groovy order=source,resultIn,resultIcaseIn,resultNotIn,resultIcaseNotIn,resultIncludesAny,resultIncludesAll
randName = {
  def names = ["Alex", "Bethany", "Yvette", "Samantha", "Ned"]
  return names[new Random().nextInt(names.size())]
}

source = emptyTable(10).update("Name = (String)randName()")
resultIn = source.where("Name in `Alex`,`Bethany`")
resultIcaseIn = source.where("Name icase in `alex`,`bethany`")
resultNotIn = source.where("Name not in `Alex`,`Bethany`")
resultIcaseNotIn = source.where("Name icase not in `alex`,`bethany`")
resultIncludesAny = source.where("Name includes any `le`,`an`")
resultIncludesAll = source.where("Name includes all `a`,`t`")
```

## Combine filters

Filters can be combined [conjunctively](#conjunctive) or [disjunctively](#disjunctive).

### Conjunctive

Conjunctive filters return only rows that match _all_ of the specified filters. There are two ways to conjunctively combine filters:

- Pass a single query string with multiple filters separated by the `&&` operator into one of the following table operations:
  - [`where`](../reference/table-operations/filter/where.md)
  - [`whereIn`](../reference/table-operations/filter/where-in.md)
  - [`whereNotIn`](../reference/table-operations/filter/where-not-in.md)
- Pass multiple query strings into one of the following table operations:
  - [`where`](../reference/table-operations/filter/where.md)
  - [`whereIn`](../reference/table-operations/filter/where-in.md)
  - [`whereNotIn`](../reference/table-operations/filter/where-not-in.md)

### Disjunctive

Disjunctive filters return only rows that match _any_ of the specified filters. There are two ways to disjunctively combine filters:

- Pass a single query string with multiple filters separated by the `||` operator into one of the following table operations:
  - [`where`](../reference/table-operations/filter/where.md)
  - [`whereIn`](../reference/table-operations/filter/where-in.md)
  - [`whereNotIn`](../reference/table-operations/filter/where-not-in.md)
- Use [`Filter.or`](https://docs.deephaven.io/core/javadoc/io/deephaven/api/filter/Filter.html#or(io.deephaven.api.filter.Filter...)) or [`DisjunctiveFilter`](https://docs.deephaven.io/core/javadoc/io/deephaven/engine/table/impl/select/DisjunctiveFilter.html) to combine multiple filter clauses.

The following examples demonstrate both approaches to disjunctive filtering:

```groovy order=source,resultOr,resultFilterOr,resultDisjunctiveFilter
import io.deephaven.api.filter.Filter
import io.deephaven.engine.table.impl.select.DisjunctiveFilter
import io.deephaven.engine.table.impl.select.WhereFilterFactory

source = emptyTable(10).update("X = ii", "Y = randomDouble(-1, 1)")

// Using || operator in a single query string
resultOr = source.where("X < 2 || X >= 8")

// Using Filter.or with multiple Filter objects
resultFilterOr = source.where(Filter.or(Filter.from("X < 2", "X >= 8")))

// Using DisjunctiveFilter.of with WhereFilter arguments
resultDisjunctiveFilter = source.where(DisjunctiveFilter.of(
    WhereFilterFactory.getExpression("X < 2"),
    WhereFilterFactory.getExpression("X >= 8")
))
```

Using `Filter.or` or `DisjunctiveFilter` is particularly useful when you need to programmatically combine filters or when working with complex filter logic that would be difficult to express in a single query string.

The following example shows how to use `Filter.or` to combine multiple conditions across different columns:

```groovy order=source,resultDisjunctive
import io.deephaven.api.filter.Filter

source = emptyTable(100).update(
    "Category = ii % 3 == 0 ? `A` : ii % 3 == 1 ? `B` : `C`",
    "Value = randomInt(0, 100)"
)

// Create a disjunctive filter that matches rows where Category is 'A' OR Value is greater than 80
resultDisjunctive = source.where(Filter.or(Filter.from("Category == `A`", "Value > 80")))
```

## Filter performance

How you structure filter clauses can affect query performance. The following guidelines help optimize filter execution.

These guidelines provide useful rules of thumb, but they won't perform best for all filters and data combinations. Filters exercise many aspects of the Deephaven engine: each filter constructs a `RowSet` representing the rows that pass, and rowset construction can be expensive. Simple filters (like match or range filters) may take advantage of optimizations like Parquet row group statistics. To evaluate a filter, data must be read from its source (e.g., disk or a network server). For important queries, measure performance to determine the optimal order and structure of filters.

### Combine filters on the same column

When filtering the same column multiple times, combine the conditions in a single clause. This reads the column data once instead of multiple times:

```groovy order=source,resultCombined,resultSeparate
source = emptyTable(1000).update("X = randomInt(0, 100)")

// Better: reads X once
resultCombined = source.where("X > 10 && X < 90")

// Worse: reads X twice
resultSeparate = source.where("X > 10", "X < 90")
```

Both produce the same result, but `resultCombined` is more efficient because it evaluates both conditions in a single pass over the data.

### Separate filters on different columns

When filtering different columns, separating them into different clauses can improve performance. The engine evaluates each clause sequentially, so earlier filters reduce the data that later filters must examine:

```groovy order=source,resultSeparate
source = emptyTable(1000).update("Bid = randomDouble(0, 200)", "Ask = randomDouble(0, 200)")

// Each filter is evaluated independently
resultSeparate = source.where("Bid > 100", "Ask > 100")
```

> [!NOTE]
> The performance benefit of separating filters on different columns depends on the selectivity of the filters. If the first filter removes most rows, subsequent filters have less work to do. However, if filters are not very selective, combining them may perform similarly.

### Order matters

Put more selective filters first. A filter that eliminates most rows early reduces the work for subsequent filters:

```groovy order=source,resultOrdered
source = emptyTable(10000).update(
    "Category = (ii % 100 == 0) ? `rare` : `common`",
    "Value = randomInt(0, 1000)"
)

// Better: rare category filter first (eliminates ~99% of rows)
resultOrdered = source.where("Category == `rare`", "Value > 500")
```

### Keep match filters separate from formulas

Match filters (equality checks like `X == 5` or `X in 1, 2, 3`) are optimized differently than formula filters (expressions like `X > 5 && X < 10`). When combining them in a single clause, the match filter optimization may not apply:

```groovy order=source,resultSeparate,resultCombined
source = emptyTable(1000).update("Symbol = (ii % 10 == 0) ? `AAPL` : `OTHER`", "Price = randomDouble(0, 200)")

// Better: match filter is optimized independently
resultSeparate = source.where("Symbol == `AAPL`", "Price > 100 && Price < 150")

// Match filter optimization may not apply when combined with formula
resultCombined = source.where("Symbol == `AAPL` && Price > 100 && Price < 150")
```

## Filter utilities

Deephaven provides several advanced filter utilities that can improve performance in specific scenarios:

### `TailInitializationFilter`

[`TailInitializationFilter`](https://docs.deephaven.io/core/javadoc/io/deephaven/engine/table/impl/util/TailInitializationFilter.html) reduces the input size for downstream operations by limiting initialization to only the most recent rows. This is particularly useful when working with large historical datasets where you're primarily interested in the tail of the data. See the [`TailInitializationFilter`](../reference/table-operations/filter/TailInitializationFilter.md) reference page for usage examples.

### `SyncTableFilter` and `LeaderTableFilter`

[`SyncTableFilter`](https://docs.deephaven.io/core/javadoc/io/deephaven/engine/table/impl/util/SyncTableFilter.html) and [`LeaderTableFilter`](https://docs.deephaven.io/core/javadoc/io/deephaven/engine/util/LeaderTableFilter.html) help synchronize table updates across multiple dependent tables. These utilities ensure that filtered results stay consistent when dealing with related tables that update at different rates. See [Synchronize multiple tables](./synchronizing-tables.md) for usage examples and guidance on choosing between these utilities.

### `WindowCheck`

[`WindowCheck`](https://docs.deephaven.io/core/javadoc/io/deephaven/engine/util/WindowCheck.html) provides time-window filtering capabilities. For practical usage, see the [`addTimeWindow`](../reference/time/add-time-window.md) reference page, which demonstrates how to add Boolean columns that indicate whether rows fall within a specified time window.

## Related documentation

- [Built-in constants](./built-in-constants.md)
- [Built-in variables](./built-in-variables.md)
- [Built-in functions](./built-in-functions.md)
- [Query strings](./query-string-overview.md)
- [Formulas](./formulas.md)
- [How to filter table data](./use-filters.md)
- [`emptyTable`](../reference/table-operations/create/emptyTable.md)
- [`newTable`](../reference/table-operations/create/newTable.md)
