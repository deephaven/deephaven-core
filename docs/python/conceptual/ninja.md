---
title: Think like a Deephaven ninja
---

## Choose the right quotes

The Deephaven Query Language (DQL) uses three types of quotes:

- ‘ (single quote)
- " (double quote)
- ` (backtick)

Choosing the right quote can be confusing. Let’s explore where each should be used.

DQL query strings are [strings](../reference/query-language/types/strings.md). In Python, strings can be defined with either a single quote or a double quote. Both yield valid query strings, as illustrated in this example.

```python order=result
from deephaven import empty_table

result = empty_table(10).update(formulas=["X = ii", "Y=X*X"])
```

Backticks surround [strings](../reference/query-language/types/strings.md) _within_ a query string. This is easier to understand with an example.

```python order=result1,result2
from deephaven import empty_table
from deephaven import read_csv

result1 = empty_table(10).update(formulas=["X = `A`", "Y=(ii%2==0) ? `P` : `Q`"])
result2 = read_csv(
    "https://media.githubusercontent.com/media/deephaven/examples/main/CryptoCurrencyHistory/CSV/CryptoTrades_20210922.csv"
).where(filters=["Instrument in `BTC/USD`,`DOGE/USD`"])
```

In table `result1`, column X is created and set equal to the string "A", and column Y is set equal to either the string "P" or the string "Q". In table `result2`, the Cryptocurrency table is filtered to only include the rows where the Instrument column is equal to the strings "BTC/USD" or "DOGE/USD".

Times, time periods, and characters are the most complex quoting case. Similar to [strings](../reference/query-language/types/strings.md), single quotes surround [times](../reference/query-language/types/date-time.md), [time periods](../reference/query-language/types/periods.md), or characters within a query [string](../reference/query-language/types/strings.md).

```python order=result
from deephaven import empty_table

result = empty_table(10).update(
    formulas=["X = '2019-01-23T12:15 ET'", "Y = X + 'PT10S'", "Z = X + 'P1D'", "A='C'"]
)
```

In this example, column X is created and is set equal to a [date-time](../reference/query-language/types/date-time.md). Column Y is also a [date-time](../reference/query-language/types/date-time.md) and is set equal to 10 minutes after the [date-time](../reference/query-language/types/date-time.md) stored in X. Column Z is a [date-time](../reference/query-language/types/date-time.md) and is set equal to one year after the [date-time](../reference/query-language/types/date-time.md) stored in X. Column A is a character. A single quote is used both to surround the [date-time](../reference/query-language/types/date-time.md), the time difference, the [time period](../reference/query-language/types/periods.md), and the character.

> [!TIP]
> Date columns are frequently stored as strings, which use backticks.

There is one catch. You need to use double quotes for your query strings if you are going to use single quotes within the query strings.

For example:

`s1 = "This is 'OK'"`

`s2 = 'This is 'NOT''`

There is no downside to using double quotes for query strings. When in doubt, use double quotes. You can use single quotes with escapes, but they are more difficult to read.

## Looping: don’t do it!

Common programming languages, such as Python, Java, C, C++, and Go, have instructions that operate upon a single piece of data at a time. In order to operate upon multiple pieces of data, users must write loops. Deephaven is similar to [NumPy](../how-to-guides/use-numpy.md), [TensorFlow](../how-to-guides/use-tensorflow.md), or [PyTorch](../how-to-guides/use-pytorch.md), where a single command operates upon multiple pieces of data.

Because looping is so common in standard programming languages, new Deephaven users rely on loops to perform calculations. Their code often looks like:

<!--(This is a dummy code block, only intended to demonstrate the complicated syntax a user would need to write this query without DH. 'Table' has no attribute 'getColumn'). This block does not need to be fixed.-->

```python skip-test
from deephaven import merge
from deephaven import read_csv
from deephaven.time import parse_instant

source = read_csv(
    "https://media.githubusercontent.com/media/deephaven/examples/main/CryptoCurrencyHistory/CSV/crypto.csv"
)
result = None
for date in (
    source.select_distinct(formulas=["dateTime"])
    .j_object.getColumn("dateTime")
    .getDirect()
):
    for sym in (
        source.select_distinct(formulas=["coin"]).j_object.getColumn("coin").getDirect()
    ):
        temp = source.where(
            filters=[f"dateTime=parse_instant(`{date}`)", f"coin=`{sym}`"]
        )
        if result is None:
            result = temp.max_by()
        else:
            result = merge([result, temp.max_by()])
```

This code is fairly complex. It has multiple loops, multiple queries, and multiple table merges. It is slow. It also does not work correctly as a dynamic query. The loops iterate over the values in the "dateTime" and "coin" columns. If a new value is added to the "coin" column, the loop does not handle the dynamic data change, and the loops no longer iterate over the correct data. Oops.

A more experienced Deephaven user would write the same query as:

```python test-set=1 order=result
from deephaven import read_csv

result = read_csv(
    "https://media.githubusercontent.com/media/deephaven/examples/main/CryptoCurrencyHistory/CSV/crypto.csv"
).max_by(["dateTime", "coin"])
```

This piece of code is:

1. much easier to read ✅
2. faster ✅
3. dynamically updates ✅

Win, Win, Win.

Every time you write a loop or call `getColumn` in your Deephaven code, ask yourself how you can accomplish the same goal without the loop and without directly accessing the contents of the Deephaven table. Most changes will involve using key columns in [aggregations](../how-to-guides/dedicated-aggregations.md) and join columns in [joins](../how-to-guides/joins-exact-relational.md). At first, these changes will seem foreign, but soon it will become natural.

As with most rules, there are a few exceptions to the "don’t use loops" rule. Most commonly, the rule is broken to get around RAM limitations. If you are applying complex calculations to a huge dataset, your calculation may exhaust the RAM available to your job. To avoid RAM limitations, a loop can be used to break the massive dataset into smaller, more manageable datasets, which fit sequentially into RAM. Such exceptions to the rule are rare.

## High performance filters

The secret to computational efficiency is being lazy - doing as little work as possible. For Deephaven, this means operating on the minimum possible amount of data.

For most use cases, [`where`](../reference/table-operations/filter/where.md) is the most common way to filter a table. But, the same logic can be expressed in different ways.

> [!CAUTION]
> This query allocates a billion row table in memory. To see the distinction between each table's load time, run the queries separately.

```python skip-test
from deephaven import empty_table

t = empty_table(1000000000).update(formulas=["X = i", "Y = X*X"])

t1 = t.where(filters=["X > 2"]).where(filters=["X < 6"])

t2 = t.where(filters=["X > 2", "X < 6"])

t3 = t.where(filters=["X > 2 && X < 6"])
```

In this query, `t1`, `t2`, and `t3` produce the same result. However, `t3` is the most efficient, because column X is only read one time; `t1` and `t2` read column `X` twice. Furthermore, `t2` is better than `t1`, because future improvements in the Deephaven Query Language will potentially allow reorderings and other optimizations for `t2` that are not possible for `t1`.

`t3` takes less than 1 second to run.

Similarly, filtering performance is dictated by how a table is structured. It is most efficient to filter partition columns before grouping columns and grouping columns before normal columns.

Many filters can be expressed as either a conditional filter (e.g., `where("X==1 || X==2 || X==3")`) or a match filter (e.g., `where("X in 1,2,3")`). When a filter can be expressed either way, use the match filter. Deephaven can perform more advanced optimizations for match filters, and you should see higher performance.

Finally, your filtering criteria may dynamically change or may be driven by values in a table. For example, an input table may contain a list of values a user cares about. The user may add or remove items from this list during the day. [`where`](../reference/table-operations/filter/where.md) cannot handle this dynamic case. Instead, you will need to filter using [`where_in`](../reference/table-operations/filter/where-in.md) or [`where_not_in`](../reference/table-operations/filter/where-not-in.md). To see this in action, try this query.

```python order=t1,t2,t3
from deephaven import empty_table, time_table

t1 = empty_table(10).update(formulas=["X=i%2", "Y=i*i"])
t2 = time_table("PT2S").update(formulas=["X=i%2"]).last_by()
t3 = t1.where_in(filter_table=t2, cols="X")
```

> [!IMPORTANT]
> To summarize:
> When performance tuning filtering:
>
> 1. filter first, to minimize the data being computed upon,
> 2. order your filters in partition, grouping, normal order,
> 3. minimize the number of times a column is read,
> 4. combine filters within a single [`where`](../reference/table-operations/filter/where.md), instead of chaining multiple [`where`](../reference/table-operations/filter/where.md)`s together,
> 5. prefer match filters to conditional filters, and
> 6. use [`where_in`](../reference/table-operations/filter/where-in.md) and [`where_not_in`](../reference/table-operations/filter/where-not-in.md) for dynamic filtering.

## ? Is Your Friend

The [ternary conditional operator](../how-to-guides/ternary-if-how-to.md) `?` allows an if-else block to be expressed in a single line of code.

```python order=source,result default=result
from deephaven import empty_table

source = empty_table(10).update(formulas=["X=i", "Y=X*X"])
result = source.update(formulas=["Z = (X%2==0) ? Y : 42"])
```

The query string `"Z = (X%2==0) ? Y : 42"` sets `Z` equal to `Y`, if `X` is even, or equal to 42 otherwise. Using the [ternary conditional operator](../how-to-guides/ternary-if-how-to.md) results in concise queries and saves you from writing functions.

## Java Inside

Most user interactions with Deephaven happen via a programming language such as Python or Groovy, yet, under the hood, the Deephaven Query Engine is written in Java. Occasionally, it is beneficial to use the "Java inside."

```python order=t1,t2
from deephaven import new_table
from deephaven.column import string_col, int_col

t1 = new_table([int_col("X", [1, 2, 3]), string_col("Y", ["A/B", "C/D", "E/F"])])
t2 = t1.update(formulas=["A=java.lang.Math.sin(X)", "B=Y.split(`/`)[1]"])
```

In this query, Java is used twice in the query strings. First, the `sin(...)` method from the `java.lang.Math` class is used to compute column `A`. If you find a Java library that implements functionality you need, just place it on the classpath, and it is available in the query language. (See [Query Language](../how-to-guides/install-and-use-java-packages.md) for more details.) Second, Deephaven Query Language strings are `java.lang.String` objects. Here the `split(...)` method plus an array access have been used to retrieve the second token in the string. This inline use of Java String methods eliminates the need to write a custom string processing function.

Because of the deep integration of Java within the Deephaven environment, it is possible to create Java objects within a Python query. This technique isn’t often used, but it is occasionally helpful when using a Java package.

```python order=t,t1
from deephaven import empty_table
import jpy

ArrayList = jpy.get_type("java.util.ArrayList")
al = ArrayList()
al.add(1)
al.add(3)
print(al)

t = empty_table(10).update(formulas=["X=i", "Y=X*X"])
t1 = t.where(filters=["al.contains(X)"])
```

In this Python example, a Java ArrayList is created and used in a query filter. You now have the power of Python, Java, and Deephaven available within a single query!

## Clean chained queries

Query operations can be chained together to express complex ideas.

```python order=result
from deephaven import empty_table

result = (
    empty_table(10)
    .update(formulas=["X = i", "Y = X*X"])
    .where(filters=["Y > 3", "X < 2"])
)
```

While powerful, these chained queries can quickly become difficult to read. Apply some formatting to clean things up.

```python order=result
from deephaven import empty_table

result = (
    empty_table(10)
    .update(
        formulas=[
            "X = i",
            "Y = X*X",
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

If you want to include comments between your chained query operations or avoid trailing slashes, surround the code block with parentheses.

```python order=result2
from deephaven import empty_table

result2 = (
    empty_table(10)
    # Comment 1
    .update(
        formulas=[
            "X = i",
            "Y = X*X",
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

With some practice, you will get a feeling for how to format chained query operations for maximum readability.

## Related documentation

- [Create an empty table](../how-to-guides/new-and-empty-table.md#empty_table)
- [Choose the right selection method](../how-to-guides/use-select-view-update.md#choose-the-right-column-selection-method)
- [Handle nulls, infs, and NaNs](../how-to-guides/null-inf-nan.md)
- [Query strings](../how-to-guides/query-string-overview.md)
- [Work with arrays](../how-to-guides/work-with-arrays.md)
- [Work with date-times](../conceptual/time-in-deephaven.md)
- [Work with strings](../how-to-guides/work-with-strings.md)
- [Use filters](../how-to-guides/use-filters.md)
- [Use ternary-if](../how-to-guides/ternary-if-how-to.md)
