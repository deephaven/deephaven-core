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

A [query string](../how-to-guides/query-string-overview.md) uses quotes at two levels. The outer quotes are Python's own string delimiters, and the inner quotes are part of the Deephaven Query Language (DQL):

| Quote              | Where it goes                                                              | What it marks                                                                                                                                                                                                                                            |
| ------------------ | -------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `"` (double quote) | Around the whole [query string](../how-to-guides/query-string-overview.md) | A Python string. Single quotes also work here, but double quotes don't clash with the single quotes inside.                                                                                                                                              |
| `` ` `` (backtick) | Inside a [query string](../how-to-guides/query-string-overview.md)         | A [string](../reference/query-language/types/strings.md) value, such as `` `A` ``                                                                                                                                                                        |
| `'` (single quote) | Inside a [query string](../how-to-guides/query-string-overview.md)         | A [date-time](../reference/query-language/types/date-time.md), date, time, [duration](../reference/query-language/types/durations.md), [time period](../reference/query-language/types/periods.md), time zone, or character, such as `'PT10S'` or `'ET'` |

Choosing the right quote can be confusing. Let's explore where to use each one.

DQL [query strings](../how-to-guides/query-string-overview.md) are [strings](../reference/query-language/types/strings.md). In Python, you can define strings with either single or double quotes, so `"Y = X * X"` and `'Y = X * X'` are the same [query string](../how-to-guides/query-string-overview.md). This example uses double quotes, as do the other query strings on this page.

```python order=result
from deephaven import empty_table

result = empty_table(10).update(formulas=["X = ii", "Y = X * X"])
```

There is one catch. You need to use double quotes for your [query strings](../how-to-guides/query-string-overview.md) if you are going to use single quotes within the [query strings](../how-to-guides/query-string-overview.md), as the date-time and character literals later in this section do.

For example:

`s1 = "This is 'OK'"`

`s2 = 'This is 'NOT''`

There is no downside to using double quotes for [query strings](../how-to-guides/query-string-overview.md). When in doubt, use double quotes. You can use single quotes with escapes, but they are more difficult to read.

Backticks surround [strings](../reference/query-language/types/strings.md) _within_ a [query string](../how-to-guides/query-string-overview.md). This is easier to understand with an example.

```python order=result1,result2
from deephaven import empty_table
from deephaven import read_csv

result1 = empty_table(10).update(formulas=["X = `A`", "Y = (ii % 2 == 0) ? `P` : `Q`"])
result2 = read_csv(
    "https://media.githubusercontent.com/media/deephaven/examples/main/CryptoCurrencyHistory/CSV/CryptoTrades_20210922.csv"
).where(filters=["Instrument in `BTC/USD`,`DOGE/USD`"])
```

In table `result1`, the query creates column `X` and sets it to the string "A". It sets column `Y` to the string "P" in even rows and the string "Q" in odd rows, using the [ternary operator](#use-the-ternary-operator). In table `result2`, the filter keeps only the rows where the `Instrument` column equals "BTC/USD" or "DOGE/USD".

Single quotes are the most complex quoting case. Just as backticks surround [strings](../reference/query-language/types/strings.md) within a [query string](../how-to-guides/query-string-overview.md), single quotes surround [date-times](../reference/query-language/types/date-time.md), dates, times, [durations](../reference/query-language/types/durations.md), [time periods](../reference/query-language/types/periods.md), time zones such as `'ET'`, and characters.

```python order=result
from deephaven import empty_table

result = empty_table(10).update(
    formulas=[
        "X = '2019-01-23T12:15 ET'",
        "Y = X + 'PT10S'",
        "Z = X + 'P1D'",
        "A = 'C'",
    ]
)
```

In this example, the query sets column `X` to a [date-time](../reference/query-language/types/date-time.md). Column `Y` holds the [date-time](../reference/query-language/types/date-time.md) 10 seconds after `X`, and column `Z` holds the [date-time](../reference/query-language/types/date-time.md) one day after `X`. Column `A` holds a character. Single quotes surround the [date-time](../reference/query-language/types/date-time.md), the [duration](../reference/query-language/types/durations.md), the [time period](../reference/query-language/types/periods.md), and the character.

> [!TIP]
> If a date column is stored as a [string](../reference/query-language/types/strings.md) rather than a [date-time](../reference/query-language/types/date-time.md), compare it with a backtick string, such as `` Date = `2021-09-22` ``, not a single-quoted date.

## Replace loops with table operations

Common programming languages, such as Python, Java, C, C++, and Go, have instructions that operate upon a single piece of data at a time. To operate upon multiple pieces of data, users must write loops. Like [NumPy](../how-to-guides/use-numpy.md), [TensorFlow](../how-to-guides/use-tensorflow.md), and [PyTorch](../how-to-guides/use-pytorch.md), Deephaven uses [vectorized operations](./vectorization-and-recipes.md), where a single command operates upon a whole column of data.

Because looping is so common in standard programming languages, new Deephaven users rely on loops to perform calculations. Their code often looks like:

<!--(This block intentionally shows the slow loop pattern a user would write instead of table operations. It is skip-test on purpose. This block does not need to be fixed.)-->

```python skip-test
from deephaven import merge
from deephaven import read_csv

source = read_csv(
    "https://media.githubusercontent.com/media/deephaven/examples/main/CryptoCurrencyHistory/CSV/crypto.csv"
).update(formulas=["Date = toLocalDate(dateTime, 'ET')"])
result = None
for date_row in source.select_distinct(formulas=["Date"]).iter_tuple():
    for coin_row in source.select_distinct(formulas=["coin"]).iter_tuple():
        temp = source.where(
            filters=[f"Date = '{date_row.Date}'", f"coin = `{coin_row.coin}`"]
        )
        if result is None:
            result = temp.max_by()
        else:
            result = merge([result, temp.max_by()])
```

This code is fairly complex. It has multiple loops, multiple queries, and multiple table [merges](../reference/table-operations/merge/merge.md), and it is slow.

The loop version also breaks on live data. A dynamic query runs on [ticking tables](./table-types.md), whose rows change while the query runs, and it updates its results as those rows change. The loops read the `Date` and `coin` values only once. When a new value appears in either column, the loops never see it, and the result goes stale. Oops.

A more experienced Deephaven user would write the same query as:

```python order=result
from deephaven import read_csv

result = (
    read_csv(
        "https://media.githubusercontent.com/media/deephaven/examples/main/CryptoCurrencyHistory/CSV/crypto.csv"
    )
    .update(formulas=["Date = toLocalDate(dateTime, 'ET')"])
    .max_by(["Date", "coin"])
)
```

This piece of code is:

1. much easier to read ✅
2. faster ✅
3. updates when the source table ticks ✅

Win, Win, Win.

Every time you write a loop or pull values out of a table, as the [`iter_tuple`](../reference/data-import-export/iterate/iter-tuple.md) calls above do, ask yourself how you can accomplish the same goal without the loop and without directly accessing the contents of the Deephaven table. Most changes involve using key columns in [aggregations](../how-to-guides/dedicated-aggregations.md) and join columns in [joins](../how-to-guides/joins-exact-relational.md). At first, these changes seem foreign, but they soon become natural.

As with most rules, there are a few exceptions to the "don't use loops" rule. Most commonly, users break the rule to get around RAM limitations. If you are applying complex calculations to a huge dataset, your calculation may exhaust the RAM available to the Deephaven server. To avoid RAM limitations, you can use a loop to break the massive dataset into smaller datasets that fit into RAM one at a time. Such exceptions to the rule are rare.

## Write fast, correct filters

The secret to computational efficiency is being lazy — doing as little work as possible. For Deephaven, this means operating on the minimum possible amount of data. Filter as early in a query as you can, so that later operations such as [`update`](../reference/table-operations/select/update.md) and [joins](../how-to-guides/joins-exact-relational.md) run on fewer rows.

[`where`](../reference/table-operations/filter/where.md) is the most common way to filter a table, but you can express the same logic in different ways.

> [!CAUTION]
> This query allocates a billion-row table in memory. If you run it to compare how long `t1`, `t2`, and `t3` take, run each filter separately.

```python skip-test
from deephaven import empty_table

t = empty_table(1000000000).update(formulas=["X = i"])

t1 = t.where(filters=["X > 2"]).where(filters=["X < 6"])

t2 = t.where(filters=["X > 2", "X < 6"])

t3 = t.where(filters=["X > 2 && X < 6"])
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

Sometimes the values you filter on live in another table that changes while the query runs. For example, an [input table](../how-to-guides/input-tables.md) can hold the values a user cares about, and the user can add or remove values during the day. A [`where`](../reference/table-operations/filter/where.md) filter's values are fixed when you call it, so it never sees those changes. Use [`where_in`](../reference/table-operations/filter/where-in.md) or [`where_not_in`](../reference/table-operations/filter/where-not-in.md) instead.

In this example, a [time table](../how-to-guides/time-table.md) stands in for the [input table](../how-to-guides/input-tables.md), so the filter values change without anyone editing them:

```python ticking-table order=t1,t2,t3
from deephaven import empty_table, time_table

t1 = empty_table(10).update(formulas=["X = i % 2", "Y = i * i"])
t2 = time_table("PT2S").update(formulas=["X = i % 2"]).last_by()
t3 = t1.where_in(filter_table=t2, cols="X")
```

As `t2` ticks, its one row alternates between `X = 0` and `X = 1`, so `t3` switches between the rows of `t1` where `X` is 0 and the rows where `X` is 1.

> [!IMPORTANT]
> To write fast, correct filters:
>
> 1. Filter first, so later operations compute on less data.
> 2. Combine filters in a single [`where`](../reference/table-operations/filter/where.md) call so the engine can reorder them when [predicate pushdown](../how-to-guides/predicate-pushdown.md) applies, and put the most selective filter first for when it can't.
> 3. Prefer simple comparisons and [match filters](../how-to-guides/use-filters.md#match-filters) to [formula filters](../how-to-guides/filters.md#keep-match-filters-separate-from-formulas).
> 4. When the filter values come from a table that changes, use [`where_in`](../reference/table-operations/filter/where-in.md) or [`where_not_in`](../reference/table-operations/filter/where-not-in.md) instead of [`where`](../reference/table-operations/filter/where.md).

## Use the ternary operator

The [ternary operator](../how-to-guides/ternary-if-how-to.md) `? :` lets you express an if-else block in a single line of code.

```python order=source,result default=result
from deephaven import empty_table

source = empty_table(10).update(formulas=["X = i", "Y = X * X"])
result = source.update(formulas=["Z = (X % 2 == 0) ? Y : 42"])
```

The [query string](../how-to-guides/query-string-overview.md) `"Z = (X % 2 == 0) ? Y : 42"` sets `Z` to `Y` when `X` is even and to 42 otherwise. Using the [ternary operator](../how-to-guides/ternary-if-how-to.md) results in concise queries and saves you from writing functions.

## Call Java from query strings

Most users work with Deephaven through a programming language such as Python or Groovy, but the Deephaven query engine is written in Java. Occasionally, it is beneficial to use that "Java inside" directly.

```python order=t1,t2
from deephaven import new_table
from deephaven.column import string_col, int_col

t1 = new_table([int_col("X", [1, 2, 3]), string_col("Y", ["A/B", "C/D", "E/F"])])
t2 = t1.update(formulas=["A = java.lang.Math.sin(X)", "B = Y.split(`/`)[1]"])
```

This query uses Java twice in its [query strings](../how-to-guides/query-string-overview.md). First, the `sin` method from the `java.lang.Math` class computes column `A`.

Second, [DQL strings](../how-to-guides/work-with-strings.md) are `java.lang.String` objects. Here, the `split` method splits `Y` on `/`, and the `[1]` [array access](../how-to-guides/work-with-arrays.md) retrieves the second token. Calling Java `String` methods inline means you don't need to write a custom string-processing function.

If you find a Java library that implements functionality you need, place it on the classpath, and [query strings](../how-to-guides/query-string-overview.md) can call it by its fully qualified class name. For more details, see [Install and use Java packages](../how-to-guides/install-and-use-java-packages.md).

Because Java is deeply integrated with Deephaven, you can create Java objects in a Python query with [jpy](../how-to-guides/use-jpy.md). This technique isn't common, but it helps when you use a Java package.

```python order=t,t1
from deephaven import new_table
from deephaven.column import string_col
import jpy

ArrayList = jpy.get_type("java.util.ArrayList")
al = ArrayList()
al.add("B")
al.add("D")
print(al)

t = new_table([string_col("Letter", ["A", "B", "C", "D", "E"])])
t1 = t.where(filters=["al.contains(Letter)"])
```

This example uses [`jpy.get_type`](../how-to-guides/use-jpy.md#use-jpyget_type) to get the Java `ArrayList` type and builds a list of strings. The filter keeps the rows whose `Letter` is in the list, so `t1` holds the rows where `Letter` is "B" or "D". The [query string](../how-to-guides/query-string-overview.md) can refer to `al` because it can use [variables from your script](../how-to-guides/query-scope.md).

The example adds strings, not numbers, on purpose. [jpy](../how-to-guides/use-jpy.md) converts each Python string to a `java.lang.String`, so the values match the `String` column `Letter`. A small Python integer doesn't work the same way. [jpy](../how-to-guides/use-jpy.md) converts it to a `java.lang.Byte`, and a `Byte` never equals the `int` or `long` values in a numeric column.

You now have the power of Python, Java, and Deephaven available within a single query!

## Format chained queries

You can chain query operations together to express complex ideas.

```python order=result
from deephaven import empty_table

result = (
    empty_table(10)
    .update(formulas=["X = i", "Y = X * X"])
    .where(filters=["Y > 3", "X > 2"])
)
```

While powerful, these chained queries can quickly become difficult to read. The example above already starts each operation on its own line with its leading dot. To clean things up further, put each [query string](../how-to-guides/query-string-overview.md) on its own line inside the operation's list.

```python order=result
from deephaven import empty_table

result = (
    empty_table(10)
    .update(
        formulas=[
            "X = i",
            "Y = X * X",
        ]
    )
    .where(
        filters=[
            "Y > 3",
            "X > 2",
        ]
    )
)
```

Because the chain is wrapped in parentheses, you can put comments between operations without backslash line continuations.

```python order=result2
from deephaven import empty_table

result2 = (
    empty_table(10)
    # Comment 1
    .update(
        formulas=[
            "X = i",
            "Y = X * X",
        ]
    )
    # Comment 2
    .where(
        filters=[
            "Y > 3",
            "X > 2",
        ]
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
- [Create an empty table](../how-to-guides/new-and-empty-table.md#empty_table)
- [Choose a join method](../how-to-guides/joins-exact-relational.md#which-method-should-you-use)
- [Single aggregation](../how-to-guides/dedicated-aggregations.md)
- [Create a time table](../how-to-guides/time-table.md)
- [Create and use input tables](../how-to-guides/input-tables.md)
- [Data indexes](../how-to-guides/data-indexes.md)
- [Install and use Java packages](../how-to-guides/install-and-use-java-packages.md)
- [Iterate over table data](../how-to-guides/iterate-table-data.md)
- [Predicate pushdown](../how-to-guides/predicate-pushdown.md)
- [Supported Parquet formats](../how-to-guides/data-import-export/parquet-formats.md)
- [Query strings](../how-to-guides/query-string-overview.md)
- [Work with arrays](../how-to-guides/work-with-arrays.md)
- [Work with strings](../how-to-guides/work-with-strings.md)
- [Use filters](../how-to-guides/use-filters.md)
- [Filters](../how-to-guides/filters.md)
- [Use NumPy in Deephaven queries](../how-to-guides/use-numpy.md)
- [Use PyTorch in Deephaven](../how-to-guides/use-pytorch.md)
- [Use TensorFlow in Deephaven](../how-to-guides/use-tensorflow.md)
- [Use the ternary conditional operator in query strings](../how-to-guides/ternary-if-how-to.md)
- [Use variables in query strings](../how-to-guides/query-scope.md)
- [Use jpy](../how-to-guides/use-jpy.md)
- [Strings](../reference/query-language/types/strings.md)
- [Date-times](../reference/query-language/types/date-time.md)
- [Durations](../reference/query-language/types/durations.md)
- [Periods](../reference/query-language/types/periods.md)
- [Barrier](../reference/query-language/types/Barrier.md)
- [`empty_table`](../reference/table-operations/create/emptyTable.md)
- [`int_col`](../reference/table-operations/create/intCol.md)
- [`iter_tuple`](../reference/data-import-export/iterate/iter-tuple.md)
- [`last_by`](../reference/table-operations/group-and-aggregate/lastBy.md)
- [`max_by`](../reference/table-operations/group-and-aggregate/maxBy.md)
- [`merge`](../reference/table-operations/merge/merge.md)
- [`new_table`](../reference/table-operations/create/newTable.md)
- [`read_csv`](../reference/data-import-export/CSV/readCsv.md)
- [`select_distinct`](../reference/table-operations/select/select-distinct.md)
- [`string_col`](../reference/table-operations/create/stringCol.md)
- [`time_table`](../reference/table-operations/create/timeTable.md)
- [`update`](../reference/table-operations/select/update.md)
- [`where`](../reference/table-operations/filter/where.md)
- [`where_in`](../reference/table-operations/filter/where-in.md)
- [`where_not_in`](../reference/table-operations/filter/where-not-in.md)
- [`jpy` Pydoc](/core/pydoc/code/jpy.html#jpy.get_type)
- [`toLocalDate` Javadoc](/core/javadoc/io/deephaven/time/DateTimeUtils.html#toLocalDate(java.time.Instant,java.time.ZoneId))
