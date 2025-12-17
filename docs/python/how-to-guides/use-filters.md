---
title: Filter table data
sidebar_label: Filter
---

This guide covers filtering table data in Deephaven. Many different table operations can be used to filter unwanted data out of a table. Some remove data based on conditional formulas, whereas others remove data based on row positions or columns.

The following table operations remove data that does not meet the conditions set forth by one or more conditional filter formulas:

- [`where`](../reference/table-operations/filter/where.md)
- [`where_one_of`](../reference/table-operations/filter/where-one-of.md)

The following table operations remove data based on row positions:

- [`head`](../reference/table-operations/filter/head.md)
- [`tail`](../reference/table-operations/filter/tail.md)
- [`slice`](../reference/table-operations/filter/slice.md)
- [`head_pct`](../reference/table-operations/filter/head-pct.md)
- [`tail_pct`](../reference/table-operations/filter/tail-pct.md)
- [`slice_pct`](../reference/table-operations/filter/slice-pct.md)

The following table operations remove data based on equality in one or more columns of a separate table:

- [`where_in`](../reference/table-operations/filter/where-in.md)
- [`where_not_in`](../reference/table-operations/filter/where-not-in.md)

## Conditional filtering

Conditional filtering applies one or more [filters](./filters.md) to remove data that does not meet the specified criteria. Comparison formulas can use [match filters](#match-filters), [range filters](#range-filters), [string filters](#string-filters), and [regular expression filters](#regular-expression-regex-filters) to remove unwanted data. The comparison formulas used in conditional filtering must equate to either `true` or `false`. When `true`, the data is kept in the table. When `false`, the data is removed.

The following code block imports the [Iris dataset](https://en.wikipedia.org/wiki/Iris_flower_data_set) found in [Deephaven's example repository](https://github.com/deephaven/examples). Subsequent examples filter this data using comparison formulas:

```python test-set=1 order=iris
from deephaven import read_csv

iris = read_csv(
    "https://media.githubusercontent.com/media/deephaven/examples/main/Iris/csv/iris.csv"
)
```

### Match filters

Match filters set forth one of the following criteria:

- Equality (`=` and `==`): If a value is equal to another value, the result is `true`. Otherwise, the result is `false`.
- Inequality (`!=`): If a value is _not_ equal to another value, the result is `true`. Otherwise, the result is `false`.
- `in`: If a value exists within a set of values, the result is `true`. Otherwise, the result is `false`.
- `not in`: If a value does _not_ exist within a set of values, the result is `true`. Otherwise, the result is `false`.
- `icase in`: The same as `in`, but capitalization of letters is ignored. This should only be used to filter string columns.
- `icase not in`: The same as `not in`, but capitalization of letters is ignored. This should only be used to filter string columns.

The following code block applies an equality filter and an inequality filter:

```python test-set=1 order=filtered_by_sepal_width,not_virginica
filtered_by_sepal_width = iris.where(filters=["SepalWidthCM = 3.5"])
not_virginica = iris.where(filters=["Class != 'Iris-virginica'"])
```

The following code block applies `in`, `not in`, `icase in`, and `icase not in`:

```python test-set=1 order=setosa_and_virginica,not_setosa_or_virginica,virginica,not_versicolor
setosa_and_virginica = iris.where(filters=["Class in `Iris-setosa`, `Iris-virginica`"])
not_setosa_or_virginica = iris.where(
    filters=["Class not in `Iris-setosa`, `Iris-virginica`"]
)
virginica = iris.where(filters=["Class icase in `iris-virginica`"])
not_versicolor = iris.where(filters=["Class icase not in `iris-versicolor`"])
```

### Range filters

Range filters remove data that does not fall into a range of values. Range filters use any of the following operators:

- `<`: Less than
- `<=`: Less than or equal to
- `>`: Greater than
- `>=`: Greater than or equal to
- `inRange`: Checks if a value is in a given range

The following code block applies each operator to remove unwanted data:

```python test-set=1 order=sepalwidth_lessthan_3,petallength_greaterthan_2,sepallength_greaterthan_or_equalto_6,petalwidth_lessthan_or_equalto_1,petalwidth_inrange
sepalwidth_lessthan_3 = iris.where(filters=["SepalWidthCM < 3.0"])
petallength_greaterthan_2 = iris.where(filters=["PetalLengthCM > 2.0"])
sepallength_greaterthan_or_equalto_6 = iris.where(filters=["SepalLengthCM >= 6.0"])
petalwidth_lessthan_or_equalto_1 = iris.where(filters=["PetalWidthCM <= 1"])
petalwidth_inrange = iris.where(filters=["inRange(PetalWidthCM, 0, 1)"])
```

### String filters

Strings in Deephaven tables are stored as [Java strings](https://docs.oracle.com/en/java/javase/17/docs//api/java.base/java/lang/String.html). Any method that can be called on a Java string can be used to filter string data. Methods such as [`startsWith`](https://docs.oracle.com/en/java/javase/17/docs//api/java.base/java/lang/String.html#startsWith(java.lang.String)), [`endswith`](https://docs.oracle.com/en/java/javase/17/docs//api/java.base/java/lang/String.html#endsWith(java.lang.String)), [`contains`](https://docs.oracle.com/en/java/javase/17/docs//api/java.base/java/lang/String.html#contains(java.lang.CharSequence)), and [`matches`](https://docs.oracle.com/en/java/javase/17/docs//api/java.base/java/lang/String.html#matches(java.lang.String)) are particularly useful.

The following code block applies these methods to filter the `iris` table on its String column, `Class`:

```python test-set=1 order=class_startswith,class_endswith,class_contains,class_matches
class_startswith = iris.where(filters="Class.startsWith(`Iris`)")
class_endswith = iris.where(filters="Class.endsWith(`setosa`)")
class_contains = iris.where(filters="Class.contains(`vir`)")
class_matches = iris.where(filters="Class.matches(`Iris-versicolor`)")
```

### Regular expression (regex) filters

Regular expression filtering is another type of string filtering that uses [regular expressions](https://en.wikipedia.org/wiki/Regular_expression) to remove unwanted data. Deephaven's [`filters`](/core/pydoc/code/deephaven.filters.html#module-deephaven.filters) submodule enables the use of regex in filtering operations.

When performing regex filtering with the [`filters`](/core/pydoc/code/deephaven.filters.html#module-deephaven.filters) submodule, the filters are applied with a specific pattern mode. Two pattern modes are available:

- `FIND` looks for a subsequence match in the string.
- `MATCHES` matches an entire string against the given pattern.

The following code block performs regular expression filtering on the `iris` table with both pattern modes. The first finds strings that are eleven characters long, whereas the second looks for the subsequence `virginica` in the `Class` column:

```python test-set=1 order=iris_eleven_chars,iris_virginica
from deephaven.filters import PatternMode, pattern

filter_eleven_chars = pattern(PatternMode.MATCHES, "Class", "...........")
filter_regex_match = pattern(PatternMode.FIND, "Class", "virginica")

iris_eleven_chars = iris.where(filter_eleven_chars)
iris_virginica = iris.where(filter_regex_match)
```

## Combine filters

Multiple match and/or conditional statements can be combined to filter data in a table. These combinations can be either conjunctive or disjunctive.

### Conjunctive

Conjunctive filtering is used to return a table where _all_ conditional filters in a [`where`](../reference/table-operations/filter/where.md) clause return `true`. Conjunctive filtering can be achieved by passing multiple filters into [`where`](../reference/table-operations/filter/where.md) via a list, or by using the `&&` operator in a single filter.

In the following example, a conjunctive filter is applied to the `iris` table to produce a new table of only Iris setosa flowers with a petal length in a specific range.

```python test-set=1 order=conjunctive_filtered_iris
conjunctive_filtered_iris = iris.where(
    filters=["Class in `Iris-setosa`", "PetalLengthCM >= 1.3 && PetalLengthCM <= 1.6"]
)
```

### Disjunctive

Disjunctive filtering is used to return a table where _one or more_ of the statements return `true`. This can be achieved using the `||` operator in a [`where`](../reference/table-operations/filter/where.md) statement or with [`where_one_of`](../reference/table-operations/filter/where-one-of.md).

In the following example, two filters work disjunctively to return a new table where the petal length is greater than 1.9 cm or less than 1.3 cm.

```python test-set=1 order=or_filtered_iris,iris_where_one_of
or_filtered_iris = iris.where(filters=["PetalLengthCM > 1.9 || PetalWidthCM < 1.3"])
iris_where_one_of = iris.where_one_of(
    filters=["PetalLengthCM > 1.9", "PetalWidthCM < 1.3"]
)
```

## Filter one table based on another

The [`where_in`](../reference/table-operations/filter/where-in.md) and [`where_not_in`](../reference/table-operations/filter/where-not-in.md) methods enable filtering of one table based on another table. These two methods are evaluated whenever either table changes, whereas [`where`](../reference/table-operations/filter/where.md) is only evaluated when the filtered table ticks. ​In the example below, the [`where_in`](../reference/table-operations/filter/where.md) and [`where_not_in`](../reference/table-operations/filter/where-not-in.md) methods are used to find Iris virginica sepal widths that match and do not match Iris versicolor sepal widths:

```python test-set=1 order=virginica,versicolor,virginica_matching_petal_widths,virginica_non_matching_petal_widths
virginica = iris.where(filters=["Class in `Iris-virginica`"])
versicolor = iris.where(filters=["Class in `Iris-versicolor`"])
virginica_matching_petal_widths = virginica.where_in(
    filter_table=versicolor, cols=["PetalWidthCM"]
)
virginica_non_matching_petal_widths = virginica.where_not_in(
    filter_table=versicolor, cols=["PetalWidthCM"]
)
```

> [!CAUTION]
> [`where_in`](../reference/table-operations/filter/where-in.md) and [`where_not_in`](../reference/table-operations/filter/where-not-in.md) are inefficient if the filter table updates frequently.

> [!TIP]
> Unlike [`natural_join`](../reference/table-operations/join/natural-join.md), [`where_in`](../reference/table-operations/filter/where-in.md) can be used when there is more than one matching value in the right table for values in the left table. This is true of [`join`](../reference/table-operations/join/join.md) as well, but [`where_in`](../reference/table-operations/filter/where.md) is faster at returning matching rows than [`join`](../reference/table-operations/join/join.md).
>
> [`where_in`](../reference/table-operations/filter/where-in.md) only provides filtering and does not allow columns to be added from the right table. In some cases, it may be desirable to use [`where_in`](../reference/table-operations/filter/where-in.md) to filter and then [`join`](../reference/table-operations/join/join.md) to add columns from the right table. This provides similar performance to [`natural_join`](../reference/table-operations/join/natural-join.md) while still allowing matches from the right table.

## Filter by row position

Filtering by row position removes unwanted data at the top, middle, or end of a table. Row positions can be chosen on their own or by percentage of the total size of a table.

The following example uses [`head`](../reference/table-operations/filter/head.md), [`tail`](../reference/table-operations/filter/tail.md), and [`slice`](../reference/table-operations/filter/slice.md) to keep only the first, middle, and last 10 rows of `iris`, respectively.

```python test-set=1 order=iris_head,iris_slice,iris_tail
iris_head = iris.head(10)
iris_slice = iris.slice(70, 80)
iris_tail = iris.tail(10)
```

The following example uses [`head_pct`](../reference/table-operations/filter/head-pct.md), [`slice_pct`](../reference/table-operations/filter/slice-pct.md), and [`tail_pct`](../reference/table-operations/filter/tail-pct.md) to keep only 10% of the rows at the top, middle, and end of the table, respectively.

```python test-set=1 order=iris_head_pct,iris_slice_pct,iris_tail_pct
iris_head_pct = iris.head_pct(0.1)
iris_slice_pct = iris.slice_pct(0.45, 0.55)
iris_tail_pct = iris.tail_pct(0.1)
```

### Incremental Release Filter

The incremental release filter converts a static or add-only table into a ticking table that parcels out rows over time. This can be useful to simulate ticking data for development or to limit the number of rows that a complex query processes at one time. The incremental release filter takes two parameters:

- The initial number of rows to present in the resulting table.
- The number of rows to release at the beginning of each update graph cycle.

The [IncrementalReleaseFilter javadoc](https://docs.deephaven.io/core/javadoc/io/deephaven/engine/table/impl/select/IncrementalReleaseFilter.html) contains more details.

```python test-set=1 order=iris_incremental
from deephaven.filters import incremental_release

# release 10 rows initially, then release 5 rows at the beginning of each update graph cycle
iris_incremental = iris.where(incremental_release(10, 5))
```

## Related documentation

- [Filters in query strings](./filters.md)
- [Formulas in query strings](./formulas.md)
- [Operators in query strings](./operators.md)
- [equals](../reference/query-language/match-filters/equals.md)
- [`head`](../reference/table-operations/filter/head.md)
- [`head pct`](../reference/table-operations/filter/head-pct.md)
- [`icase in`](../reference/query-language/match-filters/icase-in.md)
- [`icase not in`](../reference/query-language/match-filters/icase-not-in.md)
- [`in`](../reference/query-language/match-filters/in.md)
- [`join`](../reference/table-operations/join/join.md)
- [`natural join`](../reference/table-operations/join/natural-join.md)
- [`not in`](../reference/query-language/match-filters/not-in.md)
- [`slice`](../reference/table-operations/filter/slice.md)
- [`tail`](../reference/table-operations/filter/tail.md)
- [`tail pct`](../reference/table-operations/filter/tail-pct.md)
- [`where`](../reference/table-operations/filter/where.md)
- [`where_in`](../reference/table-operations/filter/where-in.md)
- [`where_one_of`](../reference/table-operations/filter/where-one-of.md)
