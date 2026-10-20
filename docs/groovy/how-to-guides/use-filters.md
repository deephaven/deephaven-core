---
title: Filter table data
---

This guide covers filtering table data in Deephaven. Many different table operations filter data in a table. Some keep rows based on filter expressions, whereas others keep rows based on values in another table or on row positions.

Deephaven filters work on live data. When a source table ticks — which means rows are added, removed, or modified — Deephaven keeps each filtered result up to date, so a filter you write once keeps working as the data changes.

[Filters in query strings](./filters.md) covers the filter expressions themselves in more depth, including `includes` string filters, `NaN` handling in range filters, and filter performance.

The [`where`](../reference/table-operations/filter/where.md) table operation keeps rows that match one or more filter expressions. [Conditional filtering](#conditional-filtering) and [Combine filters](#combine-filters) show how to use it.

These table operations keep rows based on matching values in one or more columns of a separate table:

- [`whereIn`](../reference/table-operations/filter/where-in.md)
- [`whereNotIn`](../reference/table-operations/filter/where-not-in.md)

[Filter one table based on another](#filter-one-table-based-on-another) shows how to use them.

These table operations keep rows based on row position:

- [`head`](../reference/table-operations/filter/head.md)
- [`slice`](../reference/table-operations/filter/slice.md)
- [`tail`](../reference/table-operations/filter/tail.md)
- [`headPct`](../reference/table-operations/filter/head-pct.md)
- [`slicePct`](../reference/table-operations/filter/slice-pct.md)
- [`tailPct`](../reference/table-operations/filter/tail-pct.md)

[Filter by row position](#filter-by-row-position) shows how to use them.

The last section shows how to use an [incremental release filter](#simulate-ticking-data-with-an-incremental-release-filter) to release a table's rows over time, which simulates ticking data.

The examples on this page filter the [Iris dataset](https://en.wikipedia.org/wiki/Iris_flower_data_set) from [Deephaven's example repository](https://github.com/deephaven/examples), which the following code block imports:

```groovy test-set=1 order=iris
import static io.deephaven.csv.CsvTools.readCsv

iris = readCsv("https://media.githubusercontent.com/media/deephaven/examples/main/Iris/csv/iris.csv")
```

## Conditional filtering

Conditional filtering keeps the rows that meet one or more [filters](./filters.md). A filter is usually a query-string expression, and any expression that evaluates to `true` or `false` works. Deephaven keeps a row when the filter is `true` and excludes it when the filter is `false`. The common kinds of filters are:

- [Match filters](#match-filters)
- [Range filters](#range-filters)
- [String filters](#string-filters)
- [Regular expression filters](#regular-expression-regex-filters)

### Match filters

A match filter tests one of the following conditions:

- [Equality](../reference/query-language/match-filters/equals.md) (`=` and `==`): If a value is equal to another value, the result is `true`. Otherwise, the result is `false`.
- [Inequality](../reference/query-language/match-filters/not-equals.md) (`!=`): If a value is _not_ equal to another value, the result is `true`. Otherwise, the result is `false`.
- [`in`](../reference/query-language/match-filters/in.md): If a value exists within a set of values, the result is `true`. Otherwise, the result is `false`.
- [`not in`](../reference/query-language/match-filters/not-in.md): If a value does _not_ exist within a set of values, the result is `true`. Otherwise, the result is `false`.
- [`icase in`](../reference/query-language/match-filters/icase-in.md): The same as [`in`](../reference/query-language/match-filters/in.md), but ignores capitalization of letters. Only use this to filter string columns.
- [`icase not in`](../reference/query-language/match-filters/icase-not-in.md): The same as [`not in`](../reference/query-language/match-filters/not-in.md), but ignores capitalization of letters. Only use this to filter string columns.

The following code block applies an [equality](../reference/query-language/match-filters/equals.md) filter and an [inequality](../reference/query-language/match-filters/not-equals.md) filter:

```groovy test-set=1 order=filteredBySepalWidth,notVirginica
filteredBySepalWidth = iris.where("SepalWidthCM = 3.5")
notVirginica = iris.where("Class != `Iris-virginica`")
```

The following code block applies [`in`](../reference/query-language/match-filters/in.md), [`not in`](../reference/query-language/match-filters/not-in.md), [`icase in`](../reference/query-language/match-filters/icase-in.md), and [`icase not in`](../reference/query-language/match-filters/icase-not-in.md):

```groovy test-set=1 order=setosaAndVirginica,notSetosaOrVirginica,virginicaIcase,notVersicolor
setosaAndVirginica = iris.where("Class in `Iris-setosa`, `Iris-virginica`")
notSetosaOrVirginica = iris.where("Class not in `Iris-setosa`, `Iris-virginica`")
virginicaIcase = iris.where("Class icase in `iris-virginica`")
notVersicolor = iris.where("Class icase not in `iris-versicolor`")
```

### Range filters

Range filters keep rows where values fall within a specified range. Range filters use the following comparison operators:

- `<`: Less than
- `<=`: Less than or equal to
- `>`: Greater than
- `>=`: Greater than or equal to

The [`inRange`](../reference/query-language/query-library/auto-imported/basic.md) function checks whether a value falls in a given range, including both endpoints.

The following code block applies each operator and the [`inRange`](../reference/query-language/query-library/auto-imported/basic.md) function to filter data:

```groovy test-set=1 order=sepalWidthLessThan3,petalLengthGreaterThan2,sepalLengthGreaterThanOrEqualTo6,petalWidthLessThanOrEqualTo1,petalWidthInRange
sepalWidthLessThan3 = iris.where("SepalWidthCM < 3.0")
petalLengthGreaterThan2 = iris.where("PetalLengthCM > 2.0")
sepalLengthGreaterThanOrEqualTo6 = iris.where("SepalLengthCM >= 6.0")
petalWidthLessThanOrEqualTo1 = iris.where("PetalWidthCM <= 1")
petalWidthInRange = iris.where("inRange(PetalWidthCM, 1, 2)")
```

### String filters

Deephaven stores strings as [Java strings](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/lang/String.html). Any Java `String` method that returns a boolean can filter string data. Methods such as [`startsWith`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/lang/String.html#startsWith(java.lang.String)), [`endsWith`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/lang/String.html#endsWith(java.lang.String)), [`contains`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/lang/String.html#contains(java.lang.CharSequence)), and [`matches`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/lang/String.html#matches(java.lang.String)) are particularly useful. Deephaven's `includes any` and `includes all` filters match one or more substrings without a method call. For details, see [String filters](./filters.md#string-filters) in the filters guide.

The following code block applies these methods to filter the `iris` table on its string column, `Class`:

```groovy test-set=1 order=classStartsWith,classEndsWith,classContains,classMatches
classStartsWith = iris.where("Class.startsWith(`Iris`)")
classEndsWith = iris.where("Class.endsWith(`setosa`)")
classContains = iris.where("Class.contains(`vir`)")
classMatches = iris.where("Class.matches(`Iris-v.*`)")
```

### Regular expression (regex) filters

The [`matches`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/lang/String.html#matches(java.lang.String)) method in the previous section already takes a [regular expression](https://en.wikipedia.org/wiki/Regular_expression), but the expression must match the whole string. To build a regex filter as an object rather than in a query string, use Deephaven's [`FilterPattern`](https://docs.deephaven.io/core/javadoc/io/deephaven/api/filter/FilterPattern.html) class, and pass the filter to [`where`](../reference/table-operations/filter/where.md). Its `FIND` mode matches the expression anywhere in the string, so the expression needs no `.*` at either end.

When performing regex filtering with [`FilterPattern`](https://docs.deephaven.io/core/javadoc/io/deephaven/api/filter/FilterPattern.html), you apply a specific pattern mode from [`FilterPattern.Mode`](https://docs.deephaven.io/core/javadoc/io/deephaven/api/filter/FilterPattern.Mode.html). Two pattern modes exist:

- `FIND` looks for a subsequence match in the string.
- `MATCHES` matches an entire string against the given pattern.

The following code block performs regular expression filtering on the `iris` table with both pattern modes. The first finds strings that are eleven characters long, whereas the second looks for the subsequence `virginica` in the `Class` column:

```groovy test-set=1 order=irisElevenChars,irisVirginica
import java.util.regex.Pattern

filterElevenChars = FilterPattern.of(ColumnName.of("Class"), Pattern.compile("..........."), FilterPattern.Mode.MATCHES, false)
filterRegexMatch = FilterPattern.of(ColumnName.of("Class"), Pattern.compile("virginica"), FilterPattern.Mode.FIND, false)

irisElevenChars = iris.where(filterElevenChars)
irisVirginica = iris.where(filterRegexMatch)
```

The last argument, `invertPattern`, keeps the rows that do _not_ match when set to `true`.

## Combine filters

You can combine multiple [filters](./filters.md) to filter data in a table. These combinations can be either conjunctive or disjunctive. The order and structure of combined filters can affect performance. For details, see [Filter performance](./filters.md#filter-performance).

### Conjunctive

Conjunctive (AND) filtering returns a table where _all_ filters in a [`where`](../reference/table-operations/filter/where.md) call return `true`. Achieve conjunctive filtering by passing each filter as a separate argument to [`where`](../reference/table-operations/filter/where.md), or by using the `&&` operator in a single filter.

The following example applies a conjunctive filter to the `iris` table to produce a new table of only Iris setosa flowers with a petal length in a specific range.

```groovy test-set=1 order=conjunctiveFilteredIris
conjunctiveFilteredIris = iris.where("Class in `Iris-setosa`", "PetalLengthCM >= 1.3 && PetalLengthCM <= 1.6")
```

### Disjunctive

Disjunctive (OR) filtering returns a table where _one or more_ of the filters return `true`. You can accomplish this by using the `||` operator in a [`where`](../reference/table-operations/filter/where.md) call, or by using [`Filter.or`](https://docs.deephaven.io/core/javadoc/io/deephaven/api/filter/Filter.html#or(java.util.Collection)) to combine multiple filters.

In the following example, two filters work disjunctively to return a new table where the petal length is greater than 6.0 cm or the petal width is less than 0.2 cm.

```groovy test-set=1 order=orFilteredIris,irisFilterOr
orFilteredIris = iris.where("PetalLengthCM > 6.0 || PetalWidthCM < 0.2")
irisFilterOr = iris.where(Filter.or(Filter.from("PetalLengthCM > 6.0", "PetalWidthCM < 0.2")))
```

## Filter one table based on another

The [`whereIn`](../reference/table-operations/filter/where-in.md) and [`whereNotIn`](../reference/table-operations/filter/where-not-in.md) methods filter one table based on another table. A [`where`](../reference/table-operations/filter/where.md) result updates when its source table changes. A [`whereIn`](../reference/table-operations/filter/where-in.md) or [`whereNotIn`](../reference/table-operations/filter/where-not-in.md) result also updates when the filter table changes. The following example uses [`whereIn`](../reference/table-operations/filter/where-in.md) and [`whereNotIn`](../reference/table-operations/filter/where-not-in.md) to find Iris virginica petal widths that match and do not match Iris versicolor petal widths:

```groovy test-set=1 order=virginica,versicolor,virginicaMatchingPetalWidths,virginicaNonMatchingPetalWidths
virginica = iris.where("Class in `Iris-virginica`")
versicolor = iris.where("Class in `Iris-versicolor`")
virginicaMatchingPetalWidths = virginica.whereIn(versicolor, "PetalWidthCM")
virginicaNonMatchingPetalWidths = virginica.whereNotIn(versicolor, "PetalWidthCM")
```

In this example, `virginica` is the table being filtered (the left table) and `versicolor` is the filter table (the right table).

> [!CAUTION]
> [`whereIn`](../reference/table-operations/filter/where-in.md) and [`whereNotIn`](../reference/table-operations/filter/where-not-in.md) are inefficient if the filter table updates frequently.

> [!TIP]
> [`whereIn`](../reference/table-operations/filter/where-in.md) keeps or removes rows and never adds columns. It keeps every left-table row whose values in the match columns appear anywhere in the filter table, however many times they appear.
>
> To add columns from the right table, use an [exact or relational join](./joins-exact-relational.md) instead. By default, [`naturalJoin`](../reference/table-operations/join/natural-join.md) throws an exception when more than one right-table row matches a left-table row. [`join`](../reference/table-operations/join/join.md) allows any number of matches.

## Filter by row position

Filtering by row position removes unwanted data at the beginning, middle, or end of a table. You can specify row positions as absolute values or as a percentage of the total table size.

The following example uses [`head`](../reference/table-operations/filter/head.md), [`slice`](../reference/table-operations/filter/slice.md), and [`tail`](../reference/table-operations/filter/tail.md) to keep the first 10 rows, the middle 10 rows (positions 70 through 79 of the 150-row table), and the last 10 rows of `iris`.

```groovy test-set=1 order=irisHead,irisSlice,irisTail
irisHead = iris.head(10)
irisSlice = iris.slice(70, 80)
irisTail = iris.tail(10)
```

The following example uses [`headPct`](../reference/table-operations/filter/head-pct.md), [`slicePct`](../reference/table-operations/filter/slice-pct.md), and [`tailPct`](../reference/table-operations/filter/tail-pct.md) to keep only 10% of the rows at the beginning, middle, and end of the table, respectively.

```groovy test-set=1 order=irisHeadPct,irisSlicePct,irisTailPct
irisHeadPct = iris.headPct(0.1)
irisSlicePct = iris.slicePct(0.45, 0.55)
irisTailPct = iris.tailPct(0.1)
```

## Simulate ticking data with an incremental release filter

The filters earlier on this page keep a fixed set of rows from the static `iris` table. The [`IncrementalReleaseFilter`](https://docs.deephaven.io/core/javadoc/io/deephaven/engine/table/impl/select/IncrementalReleaseFilter.html) instead releases the rows of a [static](../conceptual/table-types.md#static-tables) or [add-only](../conceptual/table-types.md#specialization-2-add-only) table over time. The result is a ticking table, which is a table whose rows change as Deephaven processes updates.

Use it to simulate ticking data during development, or to limit the number of rows a complex query processes at one time.

The [`IncrementalReleaseFilter`](https://docs.deephaven.io/core/javadoc/io/deephaven/engine/table/impl/select/IncrementalReleaseFilter.html) constructor takes two parameters:

- The initial number of rows to present in the resulting table.
- The number of rows to release at the beginning of each [update graph](../conceptual/dag.md#update-graph-cycles) cycle, which is one round in which Deephaven processes table updates.

```groovy test-set=1 ticking-table order=null
import io.deephaven.engine.table.impl.select.IncrementalReleaseFilter

// release 10 rows initially, then release 5 rows at the beginning of each update graph cycle
irisIncremental = iris.where(new IncrementalReleaseFilter(10, 5))
```

`irisIncremental` starts with the first 10 rows of `iris` and gains 5 rows each update graph cycle until all 150 rows are released.

## Related documentation

- [Filters in query strings](./filters.md)
- [Formulas in query strings](./formulas.md)
- [Operators in query strings](./operators.md)
- [Exact and relational joins](./joins-exact-relational.md)
- [Deephaven's live DAG](../conceptual/dag.md)
- [Table types](../conceptual/table-types.md)
- [Basic functions](../reference/query-language/query-library/auto-imported/basic.md)
- [equals](../reference/query-language/match-filters/equals.md)
- [`head`](../reference/table-operations/filter/head.md)
- [`headPct`](../reference/table-operations/filter/head-pct.md)
- [`icase in`](../reference/query-language/match-filters/icase-in.md)
- [`icase not in`](../reference/query-language/match-filters/icase-not-in.md)
- [`in`](../reference/query-language/match-filters/in.md)
- [`join`](../reference/table-operations/join/join.md)
- [`naturalJoin`](../reference/table-operations/join/natural-join.md)
- [not equals](../reference/query-language/match-filters/not-equals.md)
- [`not in`](../reference/query-language/match-filters/not-in.md)
- [`slice`](../reference/table-operations/filter/slice.md)
- [`slicePct`](../reference/table-operations/filter/slice-pct.md)
- [`tail`](../reference/table-operations/filter/tail.md)
- [`tailPct`](../reference/table-operations/filter/tail-pct.md)
- [`where`](../reference/table-operations/filter/where.md)
- [`whereIn`](../reference/table-operations/filter/where-in.md)
- [`whereNotIn`](../reference/table-operations/filter/where-not-in.md)
