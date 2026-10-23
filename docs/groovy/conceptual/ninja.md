---
title: Think like a Deephaven ninja
---

This guide collects habits that experienced Deephaven users rely on. It shows you how to:

- Choose the right quotes in [query strings](../how-to-guides/query-string-overview.md).
- Replace loops with [table operations](./table-operations-overview.md).
- Write fast, correct [filters](../how-to-guides/use-filters.md).
- Use the [ternary operator](../how-to-guides/ternary-if-how-to.md).
- Call Java from [query strings](../how-to-guides/query-string-overview.md).
- Format chained queries.

Each section stands on its own, so you can read them in any order.

## Choose the right quotes

A [query string](../how-to-guides/query-string-overview.md) uses quotes at two levels. The outer quotes are Groovy's own string delimiters, and the inner quotes are part of the Deephaven Query Language (DQL):

| Quote              | Where it goes                                                              | What it marks                                                                                                                                                                                                                                            |
| ------------------ | -------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `"` (double quote) | Around the whole [query string](../how-to-guides/query-string-overview.md) | A Groovy string. Single quotes also work here, but double quotes don't clash with the single quotes inside. See the `GString` note below.                                                                                                                |
| `` ` `` (backtick) | Inside a [query string](../how-to-guides/query-string-overview.md)         | A [string](../reference/query-language/types/strings.md) value, such as `` `A` ``                                                                                                                                                                        |
| `'` (single quote) | Inside a [query string](../how-to-guides/query-string-overview.md)         | A [date-time](../reference/query-language/types/date-time.md), date, time, [duration](../reference/query-language/types/durations.md), [time period](../reference/query-language/types/periods.md), time zone, or character, such as `'PT10S'` or `'ET'` |

Choosing the right quote can be confusing. Let's explore where to use each one.

DQL [query strings](../how-to-guides/query-string-overview.md) are [strings](../reference/query-language/types/strings.md). In Groovy, you can define strings with either single or double quotes, and both yield valid [query strings](../how-to-guides/query-string-overview.md). This example writes `"X = i"` with double quotes and `'Y = X * X'` with single quotes.

```groovy order=result
result = emptyTable(10).update("X = i", 'Y = X * X')
```

There is one catch. You need to use double quotes for your [query strings](../how-to-guides/query-string-overview.md) if you are going to use single quotes within the [query strings](../how-to-guides/query-string-overview.md), as the date-time and character literals later in this section do.

For example:

`s1 = "This is 'OK'"`

`s2 = 'This is 'NOT''`

Double quotes are the safer default for [query strings](../how-to-guides/query-string-overview.md). When in doubt, use double quotes. You can use single quotes with escapes, but they are more difficult to read.

Groovy treats a double-quoted string that contains a `$` expression as a `GString`, and it replaces each `$` expression with its value before the engine sees the [query string](../how-to-guides/query-string-overview.md). To put a literal `$` in a [query string](../how-to-guides/query-string-overview.md), use single quotes or escape it as `\$`.

Backticks surround [strings](../reference/query-language/types/strings.md) _within_ a [query string](../how-to-guides/query-string-overview.md). This is easier to understand with an example.

```groovy order=result1,result2
import static io.deephaven.csv.CsvTools.readCsv

result1 = emptyTable(10).update("X = `A`", 'Y = (i % 2 == 0) ? `P` : `Q`')
result2 = readCsv("https://media.githubusercontent.com/media/deephaven/examples/main/CryptoCurrencyHistory/CSV/CryptoTrades_20210922.csv")
    .where("Instrument in `BTC/USD`,`DOGE/USD`")
```

In table `result1`, the query creates column `X` and sets it to the string "A". It sets column `Y` to the string "P" in even rows and the string "Q" in odd rows, using the [ternary operator](#use-the-ternary-operator). In table `result2`, the filter keeps only the rows where the `Instrument` column equals "BTC/USD" or "DOGE/USD".

Single quotes are the most complex quoting case. Just as backticks surround [strings](../reference/query-language/types/strings.md) within a [query string](../how-to-guides/query-string-overview.md), single quotes surround [date-times](../reference/query-language/types/date-time.md), dates, times, [durations](../reference/query-language/types/durations.md), [time periods](../reference/query-language/types/periods.md), time zones such as `'ET'`, and characters.

```groovy order=result
result = emptyTable(10).update("X = '2019-01-23T12:15 ET'", "Y = X + 'PT10S'", "Z = X + 'P1D'", "A = 'C'")
```

In this example, the query sets column `X` to a [date-time](../reference/query-language/types/date-time.md). Column `Y` holds the [date-time](../reference/query-language/types/date-time.md) 10 seconds after `X`, and column `Z` holds the [date-time](../reference/query-language/types/date-time.md) one day after `X`. Column `A` holds a character. Single quotes surround the [date-time](../reference/query-language/types/date-time.md), the [duration](../reference/query-language/types/durations.md), the [time period](../reference/query-language/types/periods.md), and the character.

> [!TIP]
> If a date column is stored as a [string](../reference/query-language/types/strings.md) rather than a [date-time](../reference/query-language/types/date-time.md), compare it with a backtick string, such as `` Date = `2021-09-22` ``, not a single-quoted date.

## Replace loops with table operations

Common programming languages, such as Python, Java, C, C++, and Go, have instructions that operate upon a single piece of data at a time. To operate upon multiple pieces of data, users must write loops. Like NumPy, TensorFlow, and PyTorch, Deephaven uses [vectorized operations](./vectorization-and-recipes.md), where a single command operates upon a whole column of data.

Because looping is so common in standard programming languages, new Deephaven users rely on loops to perform calculations. Their code often looks like:

<!--(This block intentionally shows the slow loop pattern a user would write instead of table operations. It is skip-test on purpose. This block does not need to be fixed.)-->

```groovy skip-test
import static io.deephaven.csv.CsvTools.readCsv
import io.deephaven.engine.table.vectors.ColumnVectors
import java.time.LocalDate

source = readCsv("https://media.githubusercontent.com/media/deephaven/examples/main/CryptoCurrencyHistory/CSV/crypto.csv")
    .update("Date = toLocalDate(dateTime, 'ET')")
result = null

ColumnVectors.ofObject(source.selectDistinct("Date"), "Date", LocalDate).each { date ->
    ColumnVectors.ofObject(source.selectDistinct("coin"), "coin", String).each { sym ->
        temp = source.where("Date = '$date'", "coin = `$sym`")

        if (result == null) {
            result = temp.maxBy()
        }
        else {
            result = merge(result, temp.maxBy())
        }
    }
}
```

This code is fairly complex. It has multiple loops, multiple queries, and multiple table [merges](../reference/table-operations/merge/merge.md), and it is slow.

The loop version also breaks on live data. A dynamic query runs on [ticking tables](./table-types.md), whose rows change while the query runs, and it updates its results as those rows change. The loops read the `Date` and `coin` values only once. When a new value appears in either column, the loops never see it, and the result goes stale. Oops.

A more experienced Deephaven user would write the same query as:

```groovy order=result
import static io.deephaven.csv.CsvTools.readCsv

result = readCsv("https://media.githubusercontent.com/media/deephaven/examples/main/CryptoCurrencyHistory/CSV/crypto.csv")
    .update("Date = toLocalDate(dateTime, 'ET')")
    .maxBy("Date", "coin")
```

This piece of code is:

1. much easier to read ✅
2. faster ✅
3. updates when the source table ticks ✅

Win, Win, Win.

Every time you write a loop or pull values out of a table, as the [`ColumnVectors`](../how-to-guides/iterate-table-vectors.md) calls above do, ask yourself how you can accomplish the same goal without the loop and without directly accessing the contents of the Deephaven table. Most changes involve using key columns in [aggregations](../how-to-guides/dedicated-aggregations.md) and join columns in [joins](../how-to-guides/joins-exact-relational.md). At first, these changes seem foreign, but they soon become natural.

As with most rules, there are a few exceptions to the "don't use loops" rule. Most commonly, users break the rule to get around RAM limitations. If you are applying complex calculations to a huge dataset, your calculation may exhaust the RAM available to the Deephaven server. To avoid RAM limitations, you can use a loop to break the massive dataset into smaller datasets that fit into RAM one at a time. Such exceptions to the rule are rare.

## Write fast, correct filters

The secret to computational efficiency is being lazy — doing as little work as possible. For Deephaven, this means operating on the minimum possible amount of data. Filter as early in a query as you can, so that later operations such as [`update`](../reference/table-operations/select/update.md) and [joins](../how-to-guides/joins-exact-relational.md) run on fewer rows.

[`where`](../reference/table-operations/filter/where.md) is the most common way to filter a table, but you can express the same logic in different ways.

> [!CAUTION]
> This query allocates a billion-row table in memory. If you run it to compare how long `t1`, `t2`, and `t3` take, run each filter separately.

```groovy skip-test
t = emptyTable(1_000_000_000).update("X = i")

t1 = t.where("X > 2").where("X < 6")

t2 = t.where("X > 2", "X < 6")

t3 = t.where("X > 2 && X < 6")
```

In this query, `t1`, `t2`, and `t3` produce the same result, but `t2` is the best way to write it:

- In `t1` and `t2`, each simple comparison, such as `X > 2`, becomes a [range filter](../how-to-guides/use-filters.md#range-filters), which the engine evaluates without compiling a formula. In `t3`, the engine compiles the combined expression `X > 2 && X < 6` into a single [formula filter](../how-to-guides/filters.md#keep-match-filters-separate-from-formulas).
- `t2` passes both filters to one [`where`](../reference/table-operations/filter/where.md), so it produces one result table, while `t1` builds an intermediate table and filters it again. On tables that support [predicate pushdown](../how-to-guides/predicate-pushdown.md), one [`where`](../reference/table-operations/filter/where.md) also lets the engine reorder the filters.

You can write many filters as either a [formula filter](../how-to-guides/filters.md#keep-match-filters-separate-from-formulas) (e.g., `where("X == 1 || X == 2 || X == 3")`) or a [match filter](../how-to-guides/use-filters.md#match-filters) (e.g., `where("X in 1,2,3")`). When a filter can be expressed either way, use the [match filter](../how-to-guides/use-filters.md#match-filters). Deephaven evaluates a [match filter](../how-to-guides/use-filters.md#match-filters) by comparing values directly instead of compiling a formula, so [match filters](../how-to-guides/use-filters.md#match-filters) typically perform better.

Filter performance also depends on how a table is structured. Some columns let a filter skip data without reading every row. Two common kinds are:

- A [partitioning column](../how-to-guides/data-import-export/parquet-formats.md#parquet-file-directories) is a column whose values split the table's data into separate files or directories on disk. Each partition holds one value, so the engine checks that value once and skips partitions that don't match.
- An indexed column is a column with a [data index](../how-to-guides/data-indexes.md), which maps each key value to the rows that hold it. The engine looks up matching rows in the index instead of checking every row.

Within one [`where`](../reference/table-operations/filter/where.md), the engine can change the order of the filters when the data supports it:

- It applies filters on [partitioning columns](../how-to-guides/data-import-export/parquet-formats.md#parquet-file-directories) first.
- It can move filters that support [predicate pushdown](../how-to-guides/predicate-pushdown.md) ahead of others by estimated cost, and the estimate takes [data indexes](../how-to-guides/data-indexes.md) into account.

When the engine can't reorder filters, it applies them in the order you give them, so put the filter that removes the most rows first. [Predicate pushdown](../how-to-guides/predicate-pushdown.md) lists the order in which the engine prioritizes the filters it can reorder.

To keep every filter in the order you give, even when the engine could reorder them, use [Barriers](../reference/query-language/types/Barrier.md) or [serial filters](./query-engine/parallelization.md#controlling-concurrency-for-select-update-and-where).

Sometimes the values you filter on live in another table that changes while the query runs. For example, an [input table](../how-to-guides/input-tables.md) can hold the values a user cares about, and the user can add or remove values during the day. A [`where`](../reference/table-operations/filter/where.md) filter's values are fixed when you call it, so it never sees those changes. Use [`whereIn`](../reference/table-operations/filter/where-in.md) or [`whereNotIn`](../reference/table-operations/filter/where-not-in.md) instead.

In this example, a [time table](../how-to-guides/time-table.md) stands in for the [input table](../how-to-guides/input-tables.md), so the filter values change without anyone editing them:

```groovy ticking-table order=t1,t2,t3
t1 = emptyTable(10).update("X = i % 2", "Y = i * i")
t2 = timeTable("PT2S").update("X = i % 2").lastBy()
t3 = t1.whereIn(t2, "X")
```

As `t2` ticks, its one row alternates between `X = 0` and `X = 1`, so `t3` switches between the rows of `t1` where `X` is 0 and the rows where `X` is 1.

> [!IMPORTANT]
> To write fast, correct filters:
>
> 1. Filter first, so later operations compute on less data.
> 2. Combine filters in a single [`where`](../reference/table-operations/filter/where.md) call so the engine can reorder them when [predicate pushdown](../how-to-guides/predicate-pushdown.md) applies, and put the most selective filter first for when it can't.
> 3. Prefer simple comparisons and [match filters](../how-to-guides/use-filters.md#match-filters) to [formula filters](../how-to-guides/filters.md#keep-match-filters-separate-from-formulas).
> 4. When the filter values come from a table that changes, use [`whereIn`](../reference/table-operations/filter/where-in.md) or [`whereNotIn`](../reference/table-operations/filter/where-not-in.md) instead of [`where`](../reference/table-operations/filter/where.md).

## Use the ternary operator

The [ternary operator](../how-to-guides/ternary-if-how-to.md) `? :` lets you express an if-else block in a single line of code.

```groovy order=source,result default=result
source = emptyTable(10).update("X = i", "Y = X * X")
result = source.update("Z = (X % 2 == 0) ? Y : 42")
```

The [query string](../how-to-guides/query-string-overview.md) `"Z = (X % 2 == 0) ? Y : 42"` sets `Z` to `Y` when `X` is even and to 42 otherwise. Using the [ternary operator](../how-to-guides/ternary-if-how-to.md) results in concise queries and saves you from writing functions.

## Call Java from query strings

Most users work with Deephaven through a programming language such as Python or Groovy, but the Deephaven query engine is written in Java. Occasionally, it is beneficial to use that "Java inside" directly.

```groovy order=t1,t2
t1 = newTable(intCol("X", 1, 2, 3), stringCol("Y", "A/B", "C/D", "E/F"))
t2 = t1.update("A = java.lang.Math.sin(X)", "B = Y.split(`/`)[1]")
```

This query uses Java twice in its [query strings](../how-to-guides/query-string-overview.md). First, the `sin` method from the `java.lang.Math` class computes column `A`.

Second, [DQL strings](../how-to-guides/work-with-strings.md) are `java.lang.String` objects. Here, the `split` method splits `Y` on `/`, and the `[1]` [array access](../how-to-guides/work-with-arrays.md) retrieves the second token. Calling Java `String` methods inline means you don't need to write a custom string-processing function.

If you find a Java library that implements functionality you need, place it on the classpath, and [query strings](../how-to-guides/query-string-overview.md) can call it by its fully qualified class name. For more details, see [Install and use Java packages](../how-to-guides/install-and-use-java-packages.md).

Because Java is deeply integrated with Deephaven, you can create Java objects in a Groovy query. This technique isn't common, but it helps when you use a Java package.

```groovy order=t,t1
import java.util.ArrayList

al = new ArrayList()
al.add(1)
al.add(3)
println(al)

t = emptyTable(10).update("X = i", "Y = X * X")
t1 = t.where("al.contains(X)")
```

This Groovy example creates a Java `ArrayList` holding 1 and 3 and uses it in a query filter, so `t1` keeps the rows where `X` is 1 or 3. The [query string](../how-to-guides/query-string-overview.md) can refer to `al` because it can use [variables from your script](../how-to-guides/query-scope.md).

You now have the power of Groovy, Java, and Deephaven available within a single query!

## Format chained queries

You can chain query operations together to express complex ideas.

```groovy order=result
result = emptyTable(10).update("X = i", "Y = X * X").where("Y > 3", "X > 2")
```

While powerful, these chained queries can quickly become difficult to read. To clean things up, start each operation on its own line with its leading dot, and put each [query string](../how-to-guides/query-string-overview.md) on its own line.

```groovy order=result
result = emptyTable(10)
    .update(
        "X = i",
        "Y = X * X"
    )
    .where(
        "Y > 3",
        "X > 2"
    )
```

To put comments between your chained query operations, place each comment on its own line. This example also wraps the chain in parentheses so its start and end are easy to see.

```groovy order=result2
result2 = (
    emptyTable(10)
        // Comment 1
        .update(
            "X = i",
            "Y = X * X"
        )
        // Comment 2
        .where(
            "Y > 3",
            "X > 2"
        )
)
```

With some practice, you develop a feel for how to format chained query operations for maximum readability.

## Related documentation

- [Table types](./table-types.md)
- [Table operations overview](./table-operations-overview.md)
- [Work with time](./time-in-deephaven.md)
- [Parallelization](./query-engine/parallelization.md)
- [Vectorization and the recipe paradigm](./vectorization-and-recipes.md)
- [Create an empty table](../how-to-guides/new-and-empty-table.md#emptytable)
- [Choose a join method](../how-to-guides/joins-exact-relational.md#which-method-should-you-use)
- [Single aggregation](../how-to-guides/dedicated-aggregations.md)
- [Create a time table](../how-to-guides/time-table.md)
- [Create and use input tables](../how-to-guides/input-tables.md)
- [Data indexes](../how-to-guides/data-indexes.md)
- [Install and use Java packages](../how-to-guides/install-and-use-java-packages.md)
- [Iterate over tables with ColumnVectors](../how-to-guides/iterate-table-vectors.md)
- [Predicate pushdown](../how-to-guides/predicate-pushdown.md)
- [Supported Parquet formats](../how-to-guides/data-import-export/parquet-formats.md)
- [Query strings](../how-to-guides/query-string-overview.md)
- [Work with arrays](../how-to-guides/work-with-arrays.md)
- [Work with strings](../how-to-guides/work-with-strings.md)
- [Use filters](../how-to-guides/use-filters.md)
- [Filters](../how-to-guides/filters.md)
- [Use the ternary conditional operator in query strings](../how-to-guides/ternary-if-how-to.md)
- [Use variables in query strings](../how-to-guides/query-scope.md)
- [Strings](../reference/query-language/types/strings.md)
- [Date-times](../reference/query-language/types/date-time.md)
- [Durations](../reference/query-language/types/durations.md)
- [Periods](../reference/query-language/types/periods.md)
- [Barrier](../reference/query-language/types/Barrier.md)
- [`emptyTable`](../reference/table-operations/create/emptyTable.md)
- [`intCol`](../reference/table-operations/create/intCol.md)
- [`lastBy`](../reference/table-operations/group-and-aggregate/lastBy.md)
- [`maxBy`](../reference/table-operations/group-and-aggregate/maxBy.md)
- [`merge`](../reference/table-operations/merge/merge.md)
- [`newTable`](../reference/table-operations/create/newTable.md)
- [`readCsv`](../reference/data-import-export/CSV/readCsv.md)
- [`selectDistinct`](../reference/table-operations/select/select-distinct.md)
- [`stringCol`](../reference/table-operations/create/stringCol.md)
- [`timeTable`](../reference/table-operations/create/timeTable.md)
- [`toLocalDate`](../reference/time/datetime/toLocalDate.md)
- [`update`](../reference/table-operations/select/update.md)
- [`where`](../reference/table-operations/filter/where.md)
- [`whereIn`](../reference/table-operations/filter/where-in.md)
- [`whereNotIn`](../reference/table-operations/filter/where-not-in.md)
- [`ColumnVectors` Javadoc](/core/javadoc/io/deephaven/engine/table/vectors/ColumnVectors.html)
