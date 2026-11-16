---
title: Filters
---

This guide shows how to write filters in [query strings](./query-string-overview.md) and pass them to these table operations:

- [`where`](../reference/table-operations/filter/where.md)
- [`where_one_of`](../reference/table-operations/filter/where-one-of.md)

[Partitioned table](./partitioned-tables.md) operations and the [`with_filters`](../reference/table-operations/create/rollup.md#methods) method of [rollup](./rollup-table.md) and [tree](./tree-table.md) tables also accept filters.

The related [`where_in`](../reference/table-operations/filter/where-in.md) and [`where_not_in`](../reference/table-operations/filter/where-not-in.md) operations do not take filter expressions. Instead, they keep rows based on whether their values match values in another table. For an overview of every filtering operation, including filtering on another table and by row position, see [Filter table data](./use-filters.md).

This page covers the types of filters, ways to combine them, how clause structure and order affect performance, and utility classes for specialized filtering.

## What is a filter?

A filter is a [query string](./query-string-overview.md) expression that evaluates to `true` or `false`. Unlike [formulas](./formulas.md), which add new columns to a table, filters select rows. Deephaven keeps the rows where the filter is `true`.

Each filter string you pass to [`where`](../reference/table-operations/filter/where.md) is one clause: `where(["X > 2", "Y < 0"])` has two clauses, and `where("X > 2 && Y < 0")` has one.

Filters work the same on static and ticking tables. As rows are added, modified, or removed, the result of [`where`](../reference/table-operations/filter/where.md) updates to keep exactly the rows that pass.

The following preview keeps rows that match a value, fall in a range, satisfy a formula, and pass two clauses at once:

```python order=source,result_eq,result_between,result_even,result_where_multiple
from deephaven import empty_table

source = empty_table(10).update(["X = ii", "Y = randomDouble(-1, 1)"])
result_eq = source.where("X == 5")
result_between = source.where("X >= 2 && X < 6")
result_even = source.where("X % 2 == 0")
result_where_multiple = source.where(["X <= 5", "Y >= 0"])
```

## Types of filters

### Match filters

A match filter evaluates to `true` if the column value matches a value specified in the filter.

Match filters can search for a single-value match:

```python order=source,result_match,result_not_match
from deephaven import empty_table

source = empty_table(10).update("X = ii")
result_match = source.where("X == 5")
result_not_match = source.where("X != 5")
```

You can apply match filters to a column of any data type:

```python order=source,result_string_match,result_boolean_match
from deephaven import empty_table
from random import choice


def rand_name() -> str:
    return choice(["Alex", "Bethany", "Yvette", "Samantha", "Ned"])


source = empty_table(10).update(["Name = rand_name()", "BooleanCol = randomBool()"])
result_string_match = source.where("Name == `Alex`")
result_boolean_match = source.where("BooleanCol == true")
```

To match any of several values in a column of any type, use the set-inclusion match filters:

- [`in`](../reference/query-language/match-filters/in.md): Returns only rows where the column matches one of the specified values.
- [`not in`](../reference/query-language/match-filters/not-in.md): Returns only rows where the column does not match any of the specified values.

```python order=source,result_in,result_not_in
from deephaven import empty_table

source = empty_table(10).update("X = ii")
result_in = source.where("X in 2,4,6")
result_not_in = source.where("X not in 2,4,6")
```

> [!NOTE]
> Match filters created using the equality and inequality operators (`=`, [`==`](../reference/query-language/match-filters/equals.md), and [`!=`](../reference/query-language/match-filters/not-equals.md)) follow standard IEEE 754 rules for handling `NaN` values. Any comparison involving `NaN` returns `false`, except [`!=`](../reference/query-language/match-filters/not-equals.md), which returns `true` whenever either side is `NaN`.
>
> In contrast, match filters created with set-inclusion syntax ([`in`](../reference/query-language/match-filters/in.md), [`not in`](../reference/query-language/match-filters/not-in.md)) _do_ match `NaN` values. For example, `value in NaN, 10.0` returns `true` if `value` is `NaN` or `10.0`.

### Range filters

Range filters evaluate to `true` if the column value is within a specified range. You typically apply range filters to numeric columns, but you can apply them to any column that supports comparison operators.

```python order=source,result_greater_than,result_less_than,result_between,result_in_range
from deephaven import empty_table

source = empty_table(10).update("X = ii")
result_greater_than = source.where("X > 5")
result_less_than = source.where("X < 5")
result_between = source.where("X >= 2 && X <= 6")
result_in_range = source.where("inRange(X, 2, 6)")
```

> [!NOTE]
> Range comparisons treat `null` as less than any non-null value, so `<` and `<=` include null values. To exclude them, add `!isNull(X)` as its own clause, as in `where(["!isNull(X)", "X < 10"])`. Because `X < 10` stays in its own clause, it keeps its [dedicated range filter](#keep-match-and-range-filters-in-their-own-clauses). See [`isNull`](../reference/query-language/query-library/auto-imported/basic.md).
>
> Range comparisons on floating-point values follow IEEE 754 and exclude `NaN`. To include `NaN` values, write the filter as `isNaN(value) || value < 10.0`. See [`isNaN`](../reference/query-language/query-library/auto-imported/math.md).

To filter for data that is not in a range, use the `!` operator or [disjunctively](#disjunctive) combine two separate range filters:

```python order=source,result_not_in_range_disjunctive,result_not_in_range
from deephaven import empty_table

source = empty_table(10).update("X = ii")
result_not_in_range_disjunctive = source.where("X < 2 || X > 6")
result_not_in_range = source.where("!inRange(X, 2, 6)")
```

> [!NOTE]
> These forms are only equivalent for non-`NaN` values. Under standard IEEE 754 comparison rules, [`inRange`](../reference/query-language/query-library/auto-imported/basic.md) returns `false` for `NaN`. So `!inRange(X, 2, 6)` includes `NaN` rows, but `X < 2 || X > 6` excludes them.

Writing each bound as its own clause, as in `where(["X >= 2", "X <= 6"])`, can run faster than the `&&` or [`inRange`](../reference/query-language/query-library/auto-imported/basic.md) forms. See [Keep match and range filters in their own clauses](#keep-match-and-range-filters-in-their-own-clauses).

### String filters

String filters return only rows that match the specified criteria for [string](./work-with-strings.md) columns. Because string column values are [`java.lang.String`](../reference/query-language/types/strings.md) objects, a string filter can call any of their methods.

String filtering is flexible. Beyond the exact match shown in [Match filters](#match-filters), string filters can match substrings, use regular expressions, and more. The following example shows substring and regular-expression filters:

```python order=source,result_substring_match,result_regex_match
from deephaven import empty_table
from random import choice


def rand_name() -> str:
    return choice(["Alex", "Bethany", "Yvette", "Samantha", "Ned"])


source = empty_table(10).update("Name = rand_name()")
result_substring_match = source.where("Name.contains(`an`)")
result_regex_match = source.where("Name.matches(`^[AY].*`)")
```

In addition to [`in` and `not in`](#match-filters), string columns support these filters:

- [`icase in`](../reference/query-language/match-filters/icase-in.md): Returns only rows where the string column matches one of the specified values, ignoring case.
- [`icase not in`](../reference/query-language/match-filters/icase-not-in.md): Returns only rows where the string column does not match any of the specified values, ignoring case.
- `includes any`: Returns only rows where the string column contains any of the specified substrings.
- `includes all`: Returns only rows where the string column contains all of the specified substrings.

Plain `includes` works like `includes any`. Two prefixes change how `includes` matches, and you can combine them, as in `` Name icase not includes `al` ``:

- `icase` ignores case.
- `not` keeps rows that don't contain the substring. With more than one substring, `not includes any` keeps rows that are missing at least one of them, and `not includes all` keeps rows that contain none of them.

```python order=source,result_in,result_icase_in,result_not_in,result_icase_not_in,result_includes_any,result_includes_all
from deephaven import empty_table
from random import choice


def rand_name() -> str:
    return choice(["Alex", "Bethany", "Yvette", "Samantha", "Ned"])


source = empty_table(10).update("Name = rand_name()")
result_in = source.where("Name in `Alex`,`Bethany`")
result_icase_in = source.where("Name icase in `alex`,`bethany`")
result_not_in = source.where("Name not in `Alex`,`Bethany`")
result_icase_not_in = source.where("Name icase not in `alex`,`bethany`")
result_includes_any = source.where("Name includes any `le`,`an`")
result_includes_all = source.where("Name includes all `a`,`t`")
```

## Combine filters

You can combine filters [conjunctively](#conjunctive) or [disjunctively](#disjunctive).

### Conjunctive

Conjunctive filters return only rows that match _all_ of the specified filters. You can conjunctively combine filters in the following ways:

- Pass a single [query string](./query-string-overview.md) with multiple filters separated by the `&&` operator to [`where`](../reference/table-operations/filter/where.md).
- Pass a list of [query strings](./query-string-overview.md) to [`where`](../reference/table-operations/filter/where.md).
- Combine filters with [`and_`](/core/pydoc/code/deephaven.filters.html#deephaven.filters.and_) and pass the result to [`where`](../reference/table-operations/filter/where.md).

The following example shows all three forms:

```python order=source,result_and,result_list,result_filter_and
from deephaven import empty_table
from deephaven.filters import and_

source = empty_table(10).update(["X = ii", "Y = randomDouble(-1, 1)"])

# Using the && operator in a single query string
result_and = source.where("X > 2 && Y < 0")

# Using a list of query strings
result_list = source.where(["X > 2", "Y < 0"])

# Using and_ to build a single Filter object
result_filter_and = source.where(and_(["X > 2", "Y < 0"]))
```

All three forms return the same rows, but they don't run the same way. A single `&&` string compiles to one formula filter. The list and [`and_`](/core/pydoc/code/deephaven.filters.html#deephaven.filters.and_) forms keep each match or range filter in its own clause, with its dedicated implementation, so they can run faster. See [Keep match and range filters in their own clauses](#keep-match-and-range-filters-in-their-own-clauses).

### Disjunctive

Disjunctive filters return only rows that match _any_ of the specified filters. You can disjunctively combine filters in the following ways:

- Pass a single [query string](./query-string-overview.md) with multiple filters separated by the `||` operator to [`where`](../reference/table-operations/filter/where.md).
- Pass a list of [query strings](./query-string-overview.md) to [`where_one_of`](../reference/table-operations/filter/where-one-of.md).
- Combine filters with [`or_`](/core/pydoc/code/deephaven.filters.html#deephaven.filters.or_) and pass the result to [`where`](../reference/table-operations/filter/where.md).

The following example shows all three forms:

```python order=source,result_or,result_where_one_of,result_filter_or
from deephaven import empty_table
from deephaven.filters import or_

source = empty_table(10).update(["X = ii", "Y = randomDouble(-1, 1)"])

# Using the || operator in a single query string
result_or = source.where("X < 2 || X >= 8")

# Using where_one_of with a list of query strings
result_where_one_of = source.where_one_of(["X < 2", "X >= 8"])

# Using or_ to build a single Filter object
result_filter_or = source.where(or_(["X < 2", "X >= 8"]))
```

The filters passed to [`where_one_of`](../reference/table-operations/filter/where-one-of.md) can test different columns. The following example keeps rows where `Category` is `A` or `Value` is greater than 80:

```python order=source,result_disjunctive
from deephaven import empty_table

source = empty_table(100).update(
    [
        "Category = ii % 3 == 0 ? `A` : ii % 3 == 1 ? `B` : `C`",
        "Value = randomInt(0, 100)",
    ]
)

result_disjunctive = source.where_one_of(["Category == `A`", "Value > 80"])
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

```python order=source,result_separate,result_combined
from deephaven import empty_table

source = empty_table(1000).update(
    ["Symbol = (ii % 10 == 0) ? `AAPL` : `OTHER`", "Price = randomDouble(0, 200)"]
)

# Each clause is a dedicated match or range filter
result_separate = source.where(["Symbol == `AAPL`", "Price > 100", "Price < 150"])

# The whole clause is compiled as one formula filter
result_combined = source.where("Symbol == `AAPL` && Price > 100 && Price < 150")
```

Both produce the same result.

### Put the most selective clause first

The engine evaluates clauses one after another. Earlier clauses reduce the rows that later clauses examine and the size of the [`RowSet`](/core/javadoc/io/deephaven/engine/rowset/RowSet.html) each one builds. Put the clause that removes the most rows first. If none of the clauses is very selective, the order makes little difference.

The following example puts the filter on the rare category first:

```python order=source,result_ordered
from deephaven import empty_table

source = empty_table(10000).update(
    ["Category = (ii % 100 == 0) ? `rare` : `common`", "Value = randomInt(0, 1000)"]
)

# Rare category filter first (eliminates ~99% of rows)
result_ordered = source.where(["Category == `rare`", "Value > 500"])
```

The engine keeps the order you give, with two exceptions:

- When [predicate pushdown](./predicate-pushdown.md) can estimate a cheaper order, for example from [data indexes](./data-indexes.md), [sorted](../reference/table-operations/sort/sort.md) columns, or [Parquet row group metadata](./predicate-pushdown.md#parquet-row-group-metadata), the engine reorders stateless filters. A stateless filter doesn't depend on row order or outside mutable state.
- On a table with partitioning columns, such as a partitioned Parquet or Iceberg table, the engine applies [filters that use only partitioning columns](../conceptual/query-engine/parallelization.md#stateful-partition-filters) before the other filters.

[Pushdown](./predicate-pushdown.md) reordering moves a stateless filter only within its run of adjacent stateless filters. A filter marked [stateful](../conceptual/query-engine/parallelization.md#controlling-concurrency-for-select-update-and-where) ends that run. So does a filter that uses a [Python function](./python-functions.md) or other Python object, in a Python build that isn't [free-threaded](../conceptual/query-engine/parallelization.md#controlling-concurrency-for-select-update-and-where). [Pushdown](./predicate-pushdown.md) never moves a filter that ends a run, and never moves another filter past it.

Neither kind of reordering moves a filter past a filter marked [serial](../conceptual/query-engine/parallelization.md#controlling-concurrency-for-select-update-and-where), or ahead of the filter that declares a [Barrier](../reference/query-language/types/Barrier.md) it respects.

## Filter utilities

Beyond [query-string](./query-string-overview.md) filters, Deephaven provides utility classes for specialized filtering scenarios, such as limiting initial data, keeping related tables synchronized, and marking rows within a time window.

### `TailInitializationFilter`

[`TailInitializationFilter`](/core/pydoc/code/deephaven.table.html#deephaven.table.TailInitializationFilter) keeps only the most recent rows when a query initializes, so downstream operations start from a smaller input. After that, every new row passes. Use it when a query restarts mid-day against a large [add-only](../conceptual/table-types.md#specialization-2-add-only) table and only recent rows matter at startup.

It works per partition. A partition is one region of the source table, such as one file, or one contiguous range of row keys.

- [`most_recent`](/core/pydoc/code/deephaven.table.html#deephaven.table.TailInitializationFilter.most_recent) keeps rows within a time period of each partition's newest timestamp. Each partition must be sorted by the timestamp column, with no null timestamps.
- [`most_recent_rows`](/core/pydoc/code/deephaven.table.html#deephaven.table.TailInitializationFilter.most_recent_rows) keeps the last rows of each partition by position.

See the [`TailInitializationFilter`](../reference/table-operations/filter/TailInitializationFilter.md) reference page for usage examples.

### `SyncTableFilter` and `LeaderTableFilter`

[`SyncTableFilter`](/core/javadoc/io/deephaven/engine/table/impl/util/SyncTableFilter.html) and [`LeaderTableFilter`](/core/javadoc/io/deephaven/engine/util/LeaderTableFilter.html) keep filtered results consistent across related tables that update at different rates. Both are Java classes with no Python wrapper. Call them from Python through [`jpy`](./use-jpy.md). See [Synchronize multiple tables](./synchronizing-tables.md) for usage examples and guidance on choosing between these utilities.

### `WindowCheck`

[`WindowCheck`](/core/javadoc/io/deephaven/engine/util/WindowCheck.html) doesn't filter rows itself. It adds a boolean column that indicates whether each row's timestamp falls within a specified time window, updating as rows age out of the window. You can filter on that column yourself if you want to remove out-of-window rows. Python wraps it as [`time_window`](../reference/time/time-window.md). See the [`time_window`](../reference/time/time-window.md) reference page for usage examples.

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
- [Python functions in query strings](./python-functions.md)
- [Synchronize multiple tables](./synchronizing-tables.md)
- [Use jpy](./use-jpy.md)
- [Work with strings](./work-with-strings.md)
- [`Barrier`](../reference/query-language/types/Barrier.md)
- [`empty_table`](../reference/table-operations/create/emptyTable.md)
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
- [`time_window`](../reference/time/time-window.md)
- [`update`](../reference/table-operations/select/update.md)
- [`where`](../reference/table-operations/filter/where.md)
- [`where_in`](../reference/table-operations/filter/where-in.md)
- [`where_not_in`](../reference/table-operations/filter/where-not-in.md)
- [`where_one_of`](../reference/table-operations/filter/where-one-of.md)
- [`with_filters`](../reference/table-operations/create/rollup.md#methods)
- [`deephaven.filters` Pydoc](/core/pydoc/code/deephaven.filters.html)
- [`Table.where` Pydoc](/core/pydoc/code/deephaven.table.html#deephaven.table.Table.where)
- [`TailInitializationFilter` Pydoc](/core/pydoc/code/deephaven.table.html#deephaven.table.TailInitializationFilter)
- [`time_window` Pydoc](/core/pydoc/code/deephaven.experimental.html#deephaven.experimental.time_window)
- [`SyncTableFilter` Javadoc](/core/javadoc/io/deephaven/engine/table/impl/util/SyncTableFilter.html)
- [`LeaderTableFilter` Javadoc](/core/javadoc/io/deephaven/engine/util/LeaderTableFilter.html)
- [`RowSet` Javadoc](/core/javadoc/io/deephaven/engine/rowset/RowSet.html)
