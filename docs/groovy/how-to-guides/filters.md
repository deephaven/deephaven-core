---
id: filters
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

```groovy order=source,resultNotInRangeWhereOneOf,resultNotInRangeDisjunctive,resultNotInRange
source = emptyTable(10).update("X = ii")
resultNotInRangeDisjunctive = source.where("X < 2 || X >= 6")
resultNotInRangeWhereOneOf = source.where("X < 2", "X >= 6")
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

Disjunctive filters return only rows that match _any_ of the specified filters. To disjunctively combine filters, pass a single query string with multiple filters separated by the `||` operator into one of the following table operations:

- [`where`](../reference/table-operations/filter/where.md)
- [`whereIn`](../reference/table-operations/filter/where-in.md)
- [`whereNotIn`](../reference/table-operations/filter/where-not-in.md)

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
- [`emptyTable`](../reference/table-operations/create/emptyTable.md)
- [`newTable`](../reference/table-operations/create/newTable.md)
