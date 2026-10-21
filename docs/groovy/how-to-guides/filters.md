---
title: Filters
---

This guide shows how to write filters in [query strings](./query-string-overview.md) and pass them to the [`where`](../reference/table-operations/filter/where.md) table operation. The [`wouldMatch`](../reference/table-operations/filter/would-match.md) operation also evaluates filters, but instead of removing rows, it adds a boolean column showing which rows match.

[Partitioned table](./partitioned-tables.md) operations also accept filters. The [`withFilter`](../reference/table-operations/create/rollup.md#methods) method of [rollup](./rollup-table.md) and [tree](./tree-table.md) tables takes a [`Filter`](/core/javadoc/io/deephaven/api/filter/Filter.html) object, such as ``Filter.and(Filter.from("Sym == `AAPL`"))``.

The related [`whereIn`](../reference/table-operations/filter/where-in.md) and [`whereNotIn`](../reference/table-operations/filter/where-not-in.md) operations do not take filter expressions. Instead, they keep rows based on whether their values match values in another table. For an overview of every filtering operation, including filtering on another table and by row position, see [Filter table data](./use-filters.md).

This page covers the types of filters, ways to combine them, how clause structure and order affect performance, and utility classes for specialized filtering.

## What is a filter?

A filter is a [query string](./query-string-overview.md) expression that evaluates to `true` or `false`. Unlike [formulas](./formulas.md), which add new columns to a table, filters select rows. Deephaven keeps the rows where the filter is `true`.

Each filter string you pass to [`where`](../reference/table-operations/filter/where.md) is one clause: `where("X > 2", "Y < 0")` has two clauses, and `where("X > 2 && Y < 0")` has one.

Filters work the same on static and ticking tables. As rows are added, modified, or removed, the result of [`where`](../reference/table-operations/filter/where.md) updates to keep exactly the rows that pass.

The following preview keeps rows that match a value, fall in a range, satisfy a formula, and pass two clauses at once:

```groovy order=source,resultEq,resultBetween,resultEven,resultWhereMultiple
source = emptyTable(10).update("X = ii", "Y = randomDouble(-1, 1)")
resultEq = source.where("X == 5")
resultBetween = source.where("X >= 2 && X < 6")
resultEven = source.where("X % 2 == 0")
resultWhereMultiple = source.where("X <= 5", "Y >= 0")
```

## Types of filters

### Match filters

A match filter evaluates to `true` if the column value matches a value specified in the filter.

Match filters can search for a single-value match:

```groovy order=source,resultMatch,resultNotMatch
source = emptyTable(10).update("X = ii")
resultMatch = source.where("X == 5")
resultNotMatch = source.where("X != 5")
```

You can apply match filters to a column of any data type:

```groovy order=source,resultStringMatch,resultBooleanMatch
randName = {
    def names = ["Alex", "Bethany", "Yvette", "Samantha", "Ned"]
    return names[new Random().nextInt(names.size())]
}

source = emptyTable(10).update("Name = (String)randName()", "BooleanCol = randomBool()")
resultStringMatch = source.where("Name == `Alex`")
resultBooleanMatch = source.where("BooleanCol == true")
```

To match any of several values in a column of any type, use the set-inclusion match filters:

- [`in`](../reference/query-language/match-filters/in.md): Returns only rows where the column matches one of the specified values.
- [`not in`](../reference/query-language/match-filters/not-in.md): Returns only rows where the column does not match any of the specified values.

```groovy order=source,resultIn,resultNotIn
source = emptyTable(10).update("X = ii")
resultIn = source.where("X in 2,4,6")
resultNotIn = source.where("X not in 2,4,6")
```

> [!NOTE]
> Match filters created using the equality and inequality operators (`=`, [`==`](../reference/query-language/match-filters/equals.md), and [`!=`](../reference/query-language/match-filters/not-equals.md)) follow standard IEEE 754 rules for handling `NaN` values. Any comparison involving `NaN` returns `false`, except [`!=`](../reference/query-language/match-filters/not-equals.md), which returns `true` whenever either side is `NaN`.
>
> In contrast, match filters created with set-inclusion syntax ([`in`](../reference/query-language/match-filters/in.md), [`not in`](../reference/query-language/match-filters/not-in.md)) _do_ match `NaN` values. For example, `value in NaN, 10.0` returns `true` if `value` is `NaN` or `10.0`.

### Range filters

Range filters evaluate to `true` if the column value is within a specified range. You typically apply range filters to numeric columns, but you can apply them to any column that supports comparison operators.

```groovy order=source,resultGreaterThan,resultLessThan,resultBetween,resultInRange
source = emptyTable(10).update("X = ii")
resultGreaterThan = source.where("X > 5")
resultLessThan = source.where("X < 5")
resultBetween = source.where("X >= 2 && X <= 6")
resultInRange = source.where("inRange(X, 2, 6)")
```

> [!NOTE]
> Range comparisons treat `null` as less than any non-null value, so `<` and `<=` include null values. To exclude them, add `!isNull(X)` as its own clause, as in `where("!isNull(X)", "X < 10")`. Because `X < 10` stays in its own clause, it keeps its [dedicated range filter](#keep-match-and-range-filters-in-their-own-clauses). See [`isNull`](../reference/query-language/query-library/auto-imported/basic.md).
>
> Range comparisons on floating-point values follow IEEE 754 and exclude `NaN`. To include `NaN` values, write the filter as `isNaN(value) || value < 10.0`. See [`isNaN`](../reference/query-language/query-library/auto-imported/math.md).

To filter for data that is not in a range, use the `!` operator or [disjunctively](#disjunctive) combine two separate range filters:

```groovy order=source,resultNotInRangeDisjunctive,resultNotInRange
source = emptyTable(10).update("X = ii")
resultNotInRangeDisjunctive = source.where("X < 2 || X > 6")
resultNotInRange = source.where("!inRange(X, 2, 6)")
```

> [!NOTE]
> These forms are only equivalent for non-`NaN` values. Under standard IEEE 754 comparison rules, [`inRange`](../reference/query-language/query-library/auto-imported/basic.md) returns `false` for `NaN`. So `!inRange(X, 2, 6)` includes `NaN` rows, but `X < 2 || X > 6` excludes them.

Writing each bound as its own clause, as in `where("X >= 2", "X <= 6")`, can run faster than the `&&` or [`inRange`](../reference/query-language/query-library/auto-imported/basic.md) forms. See [Keep match and range filters in their own clauses](#keep-match-and-range-filters-in-their-own-clauses).

### String filters

String filters return only rows that match the specified criteria for [string](./work-with-strings.md) columns. Because string column values are [`java.lang.String`](../reference/query-language/types/strings.md) objects, a string filter can call any of their methods.

String filtering is flexible. Beyond the exact match shown in [Match filters](#match-filters), string filters can match substrings, use regular expressions, and more. The following example shows substring and regular-expression filters:

```groovy order=source,resultSubstringMatch,resultRegexMatch
randName = {
    def names = ["Alex", "Bethany", "Yvette", "Samantha", "Ned"]
    return names[new Random().nextInt(names.size())]
}

source = emptyTable(10).update("Name = (String)randName()")
resultSubstringMatch = source.where("Name.contains(`an`)")
resultRegexMatch = source.where("Name.matches(`^[AY].*`)")
```

In addition to [`in` and `not in`](#match-filters), string columns support these filters:

- [`icase in`](../reference/query-language/match-filters/icase-in.md): Returns only rows where the string column matches one of the specified values, ignoring case.
- [`icase not in`](../reference/query-language/match-filters/icase-not-in.md): Returns only rows where the string column does not match any of the specified values, ignoring case.
- `includes any`: Returns only rows where the string column contains any of the specified substrings.
- `includes all`: Returns only rows where the string column contains all of the specified substrings.

Plain `includes` works like `includes any`. Two prefixes change how `includes` matches, and you can combine them, as in `` Name icase not includes `al` ``:

- `icase` ignores case.
- `not` keeps rows that don't contain the substring. With more than one substring, `not includes any` keeps rows that are missing at least one of them, and `not includes all` keeps rows that contain none of them.

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

You can combine filters [conjunctively](#conjunctive) or [disjunctively](#disjunctive).

### Conjunctive

Conjunctive filters return only rows that match _all_ of the specified filters. You can conjunctively combine filters in the following ways:

- Pass a single [query string](./query-string-overview.md) with multiple filters separated by the `&&` operator to [`where`](../reference/table-operations/filter/where.md).
- Pass multiple [query strings](./query-string-overview.md) to [`where`](../reference/table-operations/filter/where.md).
- Combine filters with [`Filter.and`](/core/javadoc/io/deephaven/api/filter/Filter.html#and(io.deephaven.api.filter.Filter...)) and pass the result to [`where`](../reference/table-operations/filter/where.md).

The following example shows all three forms:

```groovy order=source,resultAnd,resultMultiple,resultFilterAnd
import io.deephaven.api.filter.Filter

source = emptyTable(10).update("X = ii", "Y = randomDouble(-1, 1)")

// Using the && operator in a single query string
resultAnd = source.where("X > 2 && Y < 0")

// Using multiple query strings
resultMultiple = source.where("X > 2", "Y < 0")

// Using Filter.and to build a single Filter object
resultFilterAnd = source.where(Filter.and(Filter.from("X > 2", "Y < 0")))
```

All three forms return the same rows, but they don't run the same way. A single `&&` string compiles to one formula filter. The multiple-string and [`Filter.and`](/core/javadoc/io/deephaven/api/filter/Filter.html#and(io.deephaven.api.filter.Filter...)) forms keep each match or range filter in its own clause, with its dedicated implementation, so they can run faster. See [Keep match and range filters in their own clauses](#keep-match-and-range-filters-in-their-own-clauses).

### Disjunctive

Disjunctive filters return only rows that match _any_ of the specified filters. You can disjunctively combine filters in the following ways:

- Pass a single [query string](./query-string-overview.md) with multiple filters separated by the `||` operator to [`where`](../reference/table-operations/filter/where.md).
- Combine filters with [`Filter.or`](/core/javadoc/io/deephaven/api/filter/Filter.html#or(io.deephaven.api.filter.Filter...)) and pass the result to [`where`](../reference/table-operations/filter/where.md).

The following example shows both forms:

```groovy order=source,resultOr,resultFilterOr
import io.deephaven.api.filter.Filter

source = emptyTable(10).update("X = ii", "Y = randomDouble(-1, 1)")

// Using the || operator in a single query string
resultOr = source.where("X < 2 || X >= 8")

// Using Filter.or to build a single Filter object
resultFilterOr = source.where(Filter.or(Filter.from("X < 2", "X >= 8")))
```

Use [`Filter.or`](/core/javadoc/io/deephaven/api/filter/Filter.html#or(io.deephaven.api.filter.Filter...)) when you build filters programmatically or when the logic is hard to express in one [query string](./query-string-overview.md).

The following example shows how to use [`Filter.or`](/core/javadoc/io/deephaven/api/filter/Filter.html#or(io.deephaven.api.filter.Filter...)) to combine multiple conditions across different columns:

```groovy order=source,resultDisjunctive
import io.deephaven.api.filter.Filter

source = emptyTable(100).update(
    "Category = ii % 3 == 0 ? `A` : ii % 3 == 1 ? `B` : `C`",
    "Value = randomInt(0, 100)"
)

// Matches rows where Category is A or Value is greater than 80
resultDisjunctive = source.where(Filter.or(Filter.from("Category == `A`", "Value > 80")))
```

## Filter performance

Three things drive filter cost:

- **Reading data**: The engine reads the filtered column's data from its source, such as a disk or a network server. Filters can skip some of that reading with [predicate pushdown](./predicate-pushdown.md). Match and range filters get the most pushdown optimizations, as the next section explains.
- **Evaluating the filter**: A [formula filter](#keep-match-and-range-filters-in-their-own-clauses) evaluates its whole expression for each row it examines. Match and range filters use faster dedicated implementations.
- **Building [RowSets](/core/javadoc/io/deephaven/engine/rowset/RowSet.html)**: Each filter builds a [`RowSet`](/core/javadoc/io/deephaven/engine/rowset/RowSet.html), the set of rows that pass it, and that work can be expensive.

You control that cost with two choices, covered in the next sections: how you split filters into clauses, and the order of those clauses.

These guidelines are rules of thumb. They don't produce the fastest query for every combination of filters and data. For important queries, measure performance to find the best order and structure for your filters.

### Keep match and range filters in their own clauses

Deephaven uses a dedicated match-filter or range-filter implementation for a clause that compares one column with a value, such as `X == 5`, `X in 1, 2, 3`, or `X > 10`. Match and range filters can use [predicate pushdown](./predicate-pushdown.md) optimizations, such as binary search on [sorted](../reference/table-operations/sort/sort.md) columns and [Parquet row group metadata](./predicate-pushdown.md#parquet-row-group-metadata).

Deephaven compiles the following clauses as a formula filter, which evaluates the whole expression for each row:

- A clause that joins several conditions with `&&` or `||`.
- A function or method call, such as [`inRange`](../reference/query-language/query-library/auto-imported/basic.md) in `inRange(X, 2, 6)` or ``Name.contains(`an`)``.
- A comparison on the [row variables](./built-in-variables.md) `i`, `ii`, or `k`, such as `ii > 5`.

The set-inclusion filters ([`in`](../reference/query-language/match-filters/in.md), [`not in`](../reference/query-language/match-filters/not-in.md), [`icase in`](../reference/query-language/match-filters/icase-in.md), [`icase not in`](../reference/query-language/match-filters/icase-not-in.md)) are match filters and keep their dedicated handling. The [`includes`](#string-filters) filters (`includes any`, `includes all`, and their `icase` and `not` forms) also get a dedicated string-pattern filter rather than a formula filter.

If you put a match or range filter in the same clause as other conditions, it becomes part of that formula and loses the dedicated handling. Keep it in its own clause:

```groovy order=source,resultSeparate,resultCombined
source = emptyTable(1000).update("Symbol = (ii % 10 == 0) ? `AAPL` : `OTHER`", "Price = randomDouble(0, 200)")

// Each clause is a dedicated match or range filter
resultSeparate = source.where("Symbol == `AAPL`", "Price > 100", "Price < 150")

// The whole clause is compiled as one formula filter
resultCombined = source.where("Symbol == `AAPL` && Price > 100 && Price < 150")
```

Both produce the same result.

### Put the most selective clause first

The engine evaluates clauses one after another. Earlier clauses reduce the rows that later clauses examine and the size of the [`RowSet`](/core/javadoc/io/deephaven/engine/rowset/RowSet.html) each one builds. Put the clause that removes the most rows first. If none of the clauses is very selective, the order makes little difference.

The following example puts the filter on the rare category first:

```groovy order=source,resultOrdered
source = emptyTable(10000).update(
    "Category = (ii % 100 == 0) ? `rare` : `common`",
    "Value = randomInt(0, 1000)"
)

// Rare category filter first (eliminates ~99% of rows)
resultOrdered = source.where("Category == `rare`", "Value > 500")
```

The engine keeps the order you give, with two exceptions:

- When [predicate pushdown](./predicate-pushdown.md) can estimate a cheaper order, for example from [data indexes](./data-indexes.md), [sorted](../reference/table-operations/sort/sort.md) columns, or [Parquet row group metadata](./predicate-pushdown.md#parquet-row-group-metadata), the engine reorders stateless filters. A stateless filter doesn't depend on row order or outside mutable state.
- On a table with partitioning columns, such as a partitioned Parquet or Iceberg table, the engine applies [filters that use only partitioning columns](../conceptual/query-engine/parallelization.md#stateful-partition-filters) before the other filters.

[Pushdown](./predicate-pushdown.md) reordering moves a stateless filter only within its run of adjacent stateless filters. A filter marked [stateful](../conceptual/query-engine/parallelization.md#controlling-concurrency-for-select-update-and-where) ends that run. [Pushdown](./predicate-pushdown.md) never moves a stateful filter, and never moves another filter past it.

Neither kind of reordering moves a filter past a filter marked [serial](../conceptual/query-engine/parallelization.md#controlling-concurrency-for-select-update-and-where), or ahead of the filter that declares a [Barrier](../reference/query-language/types/Barrier.md) it respects.

## Filter utilities

Beyond [query-string](./query-string-overview.md) filters, Deephaven provides utility classes for specialized filtering scenarios, such as limiting initial data, keeping related tables synchronized, and marking rows within a time window.

### `TailInitializationFilter`

[`TailInitializationFilter`](/core/javadoc/io/deephaven/engine/table/impl/util/TailInitializationFilter.html) keeps only the most recent rows when a query initializes, so downstream operations start from a smaller input. After that, every new row passes. Use it when a query restarts mid-day against a large [add-only](../conceptual/table-types.md#specialization-2-add-only) table and only recent rows matter at startup.

It works per partition. A partition is one region of the source table, such as one file, or one contiguous range of row keys.

- [`mostRecent`](/core/javadoc/io/deephaven/engine/table/impl/util/TailInitializationFilter.html) keeps rows within a time period of each partition's newest timestamp. Each partition must be sorted by the timestamp column, with no null timestamps.
- [`mostRecentRows`](/core/javadoc/io/deephaven/engine/table/impl/util/TailInitializationFilter.html) keeps the last rows of each partition by position.

See the [`TailInitializationFilter`](../reference/table-operations/filter/TailInitializationFilter.md) reference page for usage examples.

### `SyncTableFilter` and `LeaderTableFilter`

[`SyncTableFilter`](/core/javadoc/io/deephaven/engine/table/impl/util/SyncTableFilter.html) and [`LeaderTableFilter`](/core/javadoc/io/deephaven/engine/util/LeaderTableFilter.html) keep filtered results consistent across related tables that update at different rates. See [Synchronize multiple tables](./synchronizing-tables.md) for usage examples and guidance on choosing between these utilities.

### `WindowCheck`

[`WindowCheck`](/core/javadoc/io/deephaven/engine/util/WindowCheck.html) doesn't filter rows itself. It adds a boolean column that indicates whether each row's timestamp falls within a specified time window, updating as rows age out of the window. You can filter on that column yourself if you want to remove out-of-window rows. To add the column, call [`WindowCheck.addTimeWindow`](../reference/time/add-time-window.md). Its reference page has usage examples.

## Related documentation

- [Parallelizing queries](../conceptual/query-engine/parallelization.md)
- [Table types](../conceptual/table-types.md)
- [Built-in constants](./built-in-constants.md)
- [Built-in variables](./built-in-variables.md)
- [Built-in functions](./built-in-functions.md)
- [Query strings](./query-string-overview.md)
- [Formulas](./formulas.md)
- [Filter table data](./use-filters.md)
- [Create a hierarchical rollup table](./rollup-table.md)
- [Create a hierarchical tree table](./tree-table.md)
- [Create and use partitioned tables](./partitioned-tables.md)
- [Data indexes](./data-indexes.md)
- [Predicate pushdown filtering](./predicate-pushdown.md)
- [Synchronize multiple tables](./synchronizing-tables.md)
- [Work with strings](./work-with-strings.md)
- [`addTimeWindow`](../reference/time/add-time-window.md)
- [`Barrier`](../reference/query-language/types/Barrier.md)
- [`emptyTable`](../reference/table-operations/create/emptyTable.md)
- [`==`](../reference/query-language/match-filters/equals.md)
- [`!=`](../reference/query-language/match-filters/not-equals.md)
- [`icase in`](../reference/query-language/match-filters/icase-in.md)
- [`icase not in`](../reference/query-language/match-filters/icase-not-in.md)
- [`in`](../reference/query-language/match-filters/in.md)
- [`inRange`](../reference/query-language/query-library/auto-imported/basic.md)
- [`isNaN`](../reference/query-language/query-library/auto-imported/math.md)
- [`isNull`](../reference/query-language/query-library/auto-imported/basic.md)
- [`not in`](../reference/query-language/match-filters/not-in.md)
- [`sort`](../reference/table-operations/sort/sort.md)
- [Strings](../reference/query-language/types/strings.md)
- [`TailInitializationFilter`](../reference/table-operations/filter/TailInitializationFilter.md)
- [`update`](../reference/table-operations/select/update.md)
- [`where`](../reference/table-operations/filter/where.md)
- [`whereIn`](../reference/table-operations/filter/where-in.md)
- [`whereNotIn`](../reference/table-operations/filter/where-not-in.md)
- [`withFilter`](../reference/table-operations/create/rollup.md#methods)
- [`wouldMatch`](../reference/table-operations/filter/would-match.md)
- [`Filter` Javadoc](/core/javadoc/io/deephaven/api/filter/Filter.html)
- [`SyncTableFilter` Javadoc](/core/javadoc/io/deephaven/engine/table/impl/util/SyncTableFilter.html)
- [`LeaderTableFilter` Javadoc](/core/javadoc/io/deephaven/engine/util/LeaderTableFilter.html)
- [`RowSet` Javadoc](/core/javadoc/io/deephaven/engine/rowset/RowSet.html)
- [`TailInitializationFilter` Javadoc](/core/javadoc/io/deephaven/engine/table/impl/util/TailInitializationFilter.html)
- [`WindowCheck` Javadoc](/core/javadoc/io/deephaven/engine/util/WindowCheck.html)
