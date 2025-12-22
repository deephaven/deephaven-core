---
title: Filters
---

This guide focuses on using filters within [query strings](./query-string-overview.md). Unlike [formulas](./formulas.md), which add new columns to a table, filters are boolean expressions used to create a new table that is a subset of an existing table. Filters are employed in the following table operations:

- [`where`](../reference/table-operations/filter/where.md)
- [`where_one_of`](../reference/table-operations/filter/where-one-of.md)
- [`where_in`](../reference/table-operations/filter/where-in.md)
- [`where_not_in`](../reference/table-operations/filter/where-not-in.md)

Additionally, filters can be used in [partitioned table](./partitioned-tables.md) operations.

## What is a filter?

A filter is a query string expression that evaluates to `true` or `false`. The filter expression is applied to every row in a table and can be used to create a new table that is a subset of an existing table.

Filters can be applied to columns of any data type. For instance, numeric filters can keep only rows that match a specific value, are in a range, or are even/odd:

```python order=source,result_eq,result_range,result_even,result_where_multiple
from deephaven import empty_table

source = empty_table(10).update(["X = ii", "Y = randomDouble(-1, 1)"])
result_eq = source.where("X == 5")
result_range = source.where("X >= 2 && X < 6")
result_even = source.where("X % 2 == 0")
result_where_multiple = source.where(["X <= 5", "Y >= 0"])
```

String filtering provides significant flexibility. You can search for full string matches, substring matches, use regex, and more. The following example shows several string filters:

```python order=source,result_full_match,result_substring_match,result_regex
from deephaven import empty_table
from random import choice


def rand_name() -> str:
    return choice(["Alex", "Bethany", "Yvette", "Samantha", "Ned"])


source = empty_table(10).update("Name = rand_name()")
result_full_match = source.where("Name == `Alex`")
result_substring_match = source.where("Name.contains(`an`)")
result_regex = source.where("Name.matches(`^[A,Y].*`)")
```

## Types of filters

### Match filters

A match filter evaluates to true if the column value matches one specified in the filter.

Match filters can search for a single-value match:

```python order=source,result_match,result_notmatch
from deephaven import empty_table

source = empty_table(10).update("X = ii")
result_match = source.where("X == 5")
result_notmatch = source.where("X != 5")
```

Match filters can be applied to a column of any data type:

```python order=source,result_stringmatch,result_booleanmatch
from deephaven import empty_table
from random import choice


def rand_name() -> str:
    return choice(["Alex", "Bethany", "Yvette", "Samantha", "Ned"])


source = empty_table(10).update(["Name = rand_name()", "BooleanCol = randomBool()"])
result_stringmatch = source.where("Name == `Alex`")
result_booleanmatch = source.where("BooleanCol == true")
```

Match filters can also search for multiple values. This applies to columns of any data type. Deephaven offers some special match filters to do just this:

- `in`: Returns only rows where the column matches one of the specified values.
- `not in`: Returns only rows where the column does not match any of the specified values.

```python order=source,result_in,result_notin
from deephaven import empty_table

source = empty_table(10).update("X = ii")
result_in = source.where("X in 2,4,6")
result_notin = source.where("X not in 2,4,6")
```

> [!NOTE]
> Match filters created using the equality operators (`=`, `==` or `!=`) follow standard IEEE 754 rules for handling `NaN` values. Any comparison involving `NaN` returns `false`, except for `!=`, which returns `true` for all values.
>
> In contrast, match filters created with set inclusion syntax (`in`, `not in`) _will_ match `NaN` values. For example: `value in NaN, 10.0` will return `true` if `value` is `NaN` or `10.0`. Alternatively, you can use the `isNaN(value)` function to explicitly test for NaN values such as `isNaN(value) || value < 10.0`.

### Range filters

Range filters evaluate to true if the column value is within a specified range. This type of filter is typically applied to numeric columns but can be applied to any column that supports comparison operators.

```python order=source,result_greaterthan,result_lessthan,result_range,result_inrange
from deephaven import empty_table

source = empty_table(10).update("X = ii")
result_greaterthan = source.where("X > 5")
result_lessthan = source.where("X < 5")
result_range = source.where("X >= 2 && X < 6")
result_inrange = source.where("inRange(X, 2, 6)")
```

> [!NOTE]
> Null values are considered less than any non-null value for sorting and comparison purposes. Therefore, `<` and `<=` comparisons will always include `null`. To prevent this behavior, you can add an explicit null check; for example: `!isNull(value) && value < 10`.

> [!NOTE]
> Comparison operators on floating point values follow standard IEEE 754 rules for handling `NaN` values. Any comparison involving `NaN` returns `false`, except for `!=`, which returns `true` for all values. To include `NaN` values in your comparisons, you can use the `isNaN(value)` function to explicitly test for NaN values such as `isNaN(value) || value < 10.0`.

Both `result_range` and `result_inrange` can instead be implemented by [conjunctively](#conjunctive) combining two separate range filters:

```python order=source,result_range_conjunctive
from deephaven import empty_table

source = empty_table(10).update("X = ii")
result_range_conjunctive = source.where(["X >= 2", "X < 6"])
```

You can also filter for data that is not in a range by using the `!` operator or by [disjunctively](#disjunctive) combining two separate range filters:

```python order=source,result_not_in_range_where_one_of,result_not_in_range_disjunctive,result_not_in_range
from deephaven import empty_table

source = empty_table(10).update("X = ii")
result_not_in_range_disjunctive = source.where("X < 2 || X >= 6")
result_not_in_range_where_one_of = source.where_one_of(["X < 2", "X >= 6"])
result_not_in_range = source.where("!inRange(X, 2, 6)")
```

### String filters

String filters return only rows that match the specified criteria for [string](./work-with-strings.md) columns. As such, string filters can use any [`java.lang.String`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/lang/String.html) methods.

The following example shows several string filters:

```python order=source,result_stringmatch,result_substringmatch,result_regexmatch
from deephaven import empty_table
from random import choice


def rand_name() -> str:
    return choice(["Alex", "Bethany", "Yvette", "Samantha", "Ned"])


source = empty_table(10).update(["Name = rand_name()", "BooleanCol = randomBool()"])
result_stringmatch = source.where("Name == `Alex`")
result_substringmatch = source.where("Name.contains(`an`)")
result_regexmatch = source.where("Name.matches(`^[A,Y].*`)")
```

Deephaven offers some additional special string match filters:

- `in`: Returns only rows where the string column matches one of the specified values.
- `icase in`: Returns only rows where the string column matches one of the specified values, ignoring case.
- `not in`: Returns only rows where the string column does not match any of the specified values.
- `icase not in`: Returns only rows where the string column does not match any of the specified values, ignoring case.
- `includes any`: Returns only rows where the string column contains any of the specified substrings.
- `includes all`: Returns only rows where the string column contains all of the specified substrings.

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

Filters can be combined [conjunctively](#conjunctive) or [disjunctively](#disjunctive).

### Conjunctive

Conjunctive filters return only rows that match _all_ of the specified filters. There are two ways to conjunctively combine filters:

- Pass a single query string with multiple filters separated by the `&&` operator into one of the following table operations:
  - [`where`](../reference/table-operations/filter/where.md)
  - [`where_in`](../reference/table-operations/filter/where-in.md)
  - [`where_not_in`](../reference/table-operations/filter/where-not-in.md)
- Pass multiple query strings into one of the following table operations:
  - [`where`](../reference/table-operations/filter/where.md)
  - [`where_in`](../reference/table-operations/filter/where-in.md)
  - [`where_not_in`](../reference/table-operations/filter/where-not-in.md)

### Disjunctive

Disjunctive filters return only rows that match _any_ of the specified filters. There are two ways to disjunctively combine filters:

- Pass a single query string with multiple filters separated by the `||` operator into one of the following table operations:
  - [`where`](../reference/table-operations/filter/where.md)
  - [`where_in`](../reference/table-operations/filter/where-in.md)
  - [`where_not_in`](../reference/table-operations/filter/where-not-in.md)
- Pass multiple query strings into the following table operation:
  - [`where_one_of`](../reference/table-operations/filter/where-one-of.md)

## Related documentation

- [Built-in constants](./built-in-constants.md)
- [Built-in variables](./built-in-variables.md)
- [Built-in functions](./built-in-functions.md)
- [Query strings](./query-string-overview.md)
- [Formulas](./formulas.md)
- [How to filter table data](./use-filters.md)
- [`empty_table`](../reference/table-operations/create/emptyTable.md)
- [`new_table`](../reference/table-operations/create/newTable.md)
