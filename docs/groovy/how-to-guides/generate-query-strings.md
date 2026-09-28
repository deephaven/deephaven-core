---
title: Programmatically generate query strings with Groovy
sidebar_label: Generate query strings
---

The Deephaven Query Language allows users to write very powerful queries to filter and modify tables of data. Consider the following query, which uses a [formula](./formulas.md) to add a new column to a table and a [filter](./filters.md) to filter the resulting table.

```groovy order=result,source
source = emptyTable(7).update("Value = i")

result = source.update("X = sqrt(Value) + Value").where("X > 2 && X < 8")
```

Deephaven query strings are passed into table operations as [Groovy strings](https://groovy-lang.org/syntax.html#all-strings). As such, all of the power of Groovy can be used to generate query strings. This can be convenient when working with complex queries. Let's work though a few examples that are simplified by using Groovy to generate query strings.

> [!NOTE]
> This guide assumes you are familiar with the use of [strings](https://groovy-lang.org/syntax.html#all-strings), [GStrings](https://groovy-lang.org/syntax.html#_gstring_and_string_hashcodes), [loops](https://groovy-lang.org/semantics.html#_looping_structures), and [list operations](https://groovy-lang.org/syntax.html#_lists) in Groovy. If not, please refer to the Groovy documentation for more information.

## Many columns

In practice, queries may have a large number of inputs, making it inconvenient to type in each column name. Other times, the input column names are determined by user inputs and are not known when the query is written. Both of these situations can be addressed by using a list of column names to generate queries.

In the following example, a [GString](https://groovy-lang.org/syntax.html#_gstring_and_string_hashcodes) and [`join`](https://groovy-lang.org/groovy-dev-kit.html#_working_with_collections) are used to create a query string to sum up all of the columns and then take the square root.

```groovy order=result,source
cols = ["A", "B", "C", "D"]

source = newTable(*cols.collect { col -> intCol(col, 0, 1, 2, 3, 4, 5, 6) })

result = source.update("X = sqrt(sum(${cols.join(',')}))")
```

If the list of columns changes, the query string programmatically adapts:

```groovy order=result,source
cols = ["A", "B", "C", "D", "E"]

source = newTable(*cols.collect { col -> intCol(col, 0, 1, 2, 3, 4, 5, 6) })

result = source.update("X = sqrt(sum(${cols.join(',')}))")
```

## Repeated logic

Some queries repeat the same logic -- with minor tweaks. For example, a query may add columns containing data from 1, 5, and 10 minutes ago. Generated query strings can also help simplify these situations.

In the following example, a [GString](https://groovy-lang.org/syntax.html#_gstring_and_string_hashcodes) is used to create columns of data from 1, 5, and 10 rows before.

```groovy order=result,source
source = emptyTable(100).update("X = ii")

offsets = [1, 5, 10]

result = source

for (offset in offsets) {
    result = result.update("X${offset} = X_[ii-${offset}]")
}
```

This can be simplified further by using Groovy's [`collect`](https://groovy-lang.org/groovy-dev-kit.html#_working_with_collections) method.

```groovy order=result,source
source = emptyTable(100).update("X = ii")

offsets = [1, 5, 10]
result = source.update(*offsets.collect { offset -> "X${offset} = X_[ii-${offset}]" })
```

Data analysis, particularly in finance, often involves binning data into time buckets for analysis. These queries rarely use a single time bucket to analyze the data - they often use several or more. Groovy's GStrings make queries shorter and more readable. Consider first, a query that places data into 9 different temporal buckets without GStrings:

```groovy order=result,source
source = emptyTable(100).update(
    "Timestamp = '2024-03-15T09:30:00 ET' + i * MINUTE",
    "Price = randomDouble(0, 100)",
    "Size = randomInt(0, 25)"
)

result = source.update(
    "Bin3Min = lowerBin(Timestamp, 3 * MINUTE)",
    "Bin5Min = lowerBin(Timestamp, 5 * MINUTE)",
    "Bin7Min = lowerBin(Timestamp, 7 * MINUTE)",
    "Bin10Min = lowerBin(Timestamp, 10 * MINUTE)",
    "Bin15Min = lowerBin(Timestamp, 15 * MINUTE)",
    "Bin20Min = lowerBin(Timestamp, 20 * MINUTE)",
    "Bin30Min = lowerBin(Timestamp, 30 * MINUTE)",
    "Bin45Min = lowerBin(Timestamp, 45 * MINUTE)",
    "Bin60Min = lowerBin(Timestamp, 60 * MINUTE)"
)
```

Not only was that query tedious to write, but the formatting is long and repetitive, and doesn't take advantage of Groovy's power. Consider the following query, which does the same thing, but with GStrings and the `collect` method.

```groovy order=result,source
source = emptyTable(100).update(
    "Timestamp = '2024-03-15T09:30:00 ET' + i * MINUTE",
    "Price = randomDouble(0, 100)",
    "Size = randomInt(0, 25)"
)

bin_sizes = [3, 5, 7, 10, 15, 20, 30, 45, 60]

result = source.update(
    *bin_sizes.collect { bin_size -> "Bin${bin_size}Min = lowerBin(Timestamp, ${bin_size} * MINUTE)" }
)
```

This query is shorter, faster to write, and easier to read for a Groovy programmer. It also makes future updates easier to write, and changes to the `bin_sizes` list are automatically reflected in the `result` table.

## Be creative!

Programmatically generating query strings works for all Deephaven operations, not just [`update`](../reference/table-operations/select/update.md). For example, this case uses multiple programmatically generated query strings while performing a join.

```groovy order=result,source
source = emptyTable(100).update("X = ii", "Y = X", "Z = sqrt(X)")

offsets = [1, 5, 10]

result = source

for (offset in offsets) {
    result = result.naturalJoin(
        source.update("XOffset = X+${offset}"),
        "X=XOffset",
        "Y${offset}=Y,Z${offset}=Z"
    )
}
```

## Related documentation

- [Built-in query language constants](./built-in-constants.md)
- [Built-in query language variables](./built-in-variables.md)
- [Built-in query language functions](./built-in-functions.md)
- [Create a new table](./new-and-empty-table.md#newtable)
- [Create an empty table](./new-and-empty-table.md#emptytable)
- [Formulas in query strings](./formulas.md)
- [Filters in query strings](./filters.md)
- [Operators in query strings](./operators.md)
- [Groovy variables in query strings](./groovy-variables.md)
- [Groovy closures in query strings](./groovy-closures.md)
- [Groovy classes in query strings](./groovy-classes.md)
- [Think like a Deephaven ninja](../conceptual/ninja.md)
