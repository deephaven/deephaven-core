---
title: Filter table data
sidebar_label: Filter
---

This guide covers filtering table data in Deephaven. Many different table operations filter data in a table. Some keep data based on conditional formulas, whereas others keep data based on row positions or columns.

These table operations keep rows that match one or more conditional filter formulas:

- [`where`](../reference/table-operations/filter/where.md)

These table operations keep rows based on row position:

- [`head`](../reference/table-operations/filter/head.md)
- [`tail`](../reference/table-operations/filter/tail.md)
- [`slice`](../reference/table-operations/filter/slice.md)
- [`headPct`](../reference/table-operations/filter/head-pct.md)
- [`tailPct`](../reference/table-operations/filter/tail-pct.md)
- [`slicePct`](../reference/table-operations/filter/slice-pct.md)

These table operations keep rows based on equality in one or more columns of a separate table:

- [`whereIn`](../reference/table-operations/filter/where-in.md)
- [`whereNotIn`](../reference/table-operations/filter/where-not-in.md)

## Conditional filtering

Conditional filtering applies one or more [filters](./filters.md) to keep rows that meet the specified criteria. Comparison formulas can use [match filters](#match-filters), [range filters](#range-filters), [string filters](#string-filters), and [regular expression filters](#regular-expression-regex-filters). Comparison formulas must equate to either `true` or `false`. When `true`, Deephaven keeps the row. When `false`, Deephaven excludes the row.

The following code block imports the [Iris dataset](https://en.wikipedia.org/wiki/Iris_flower_data_set) found in [Deephaven's example repository](https://github.com/deephaven/examples). Subsequent examples filter this data using comparison formulas:

```groovy test-set=1 order=iris
import static io.deephaven.csv.CsvTools.readCsv

iris = readCsv("https://media.githubusercontent.com/media/deephaven/examples/main/Iris/csv/iris.csv")
```

### Match filters

Match filters set forth one of the following criteria:

- Equality (`=` and `==`): If a value is equal to another value, the result is `true`. Otherwise, the result is `false`.
- Inequality (`!=`): If a value is _not_ equal to another value, the result is `true`. Otherwise, the result is `false`.
- `in`: If a value exists within a set of values, the result is `true`. Otherwise, the result is `false`.
- `not in`: If a value does _not_ exist within a set of values, the result is `true`. Otherwise, the result is `false`.
- `icase in`: The same as `in`, but ignores capitalization of letters. Only use this to filter string columns.
- `icase not in`: The same as `not in`, but ignores capitalization of letters. Only use this to filter string columns.

The following code block applies an equality filter and an inequality filter:

```groovy test-set=1 order=filteredBySepalWidth,notVirginica
filteredBySepalWidth = iris.where("SepalWidthCM = 3.5")
notVirginica = iris.where("Class != `Iris-virginica`")
```

The following code block applies `in`, `not in`, `icase in`, and `icase not in`:

```groovy test-set=1 order=setosaAndVirginica,notSetosaOrVirginica,virginica,notVersicolor
setosaAndVirginica = iris.where("Class in `Iris-setosa`, `Iris-virginica`")
notSetosaOrVirginica = iris.where("Class not in `Iris-setosa`, `Iris-virginica`")
virginica = iris.where("Class icase in `iris-virginica`")
notVersicolor = iris.where("Class icase not in `iris-versicolor`")
```

### Range filters

Range filters keep rows where values fall within a specified range. Range filters use any of the following operators:

- `<`: Less than
- `<=`: Less than or equal to
- `>`: Greater than
- `>=`: Greater than or equal to
- `inRange`: Checks if a value is in a given range

The following code block applies each operator to filter data:

```groovy test-set=1 order=sepalwidthLessthan3,petallenthGreaterThan2,sepallengthGreaterThanOrEqualTo6,petalwidthLessThanOrEqualTo1,petalwidthInrange
sepalwidthLessthan3 = iris.where("SepalWidthCM < 3.0")
petallenthGreaterThan2 = iris.where("PetalLengthCM > 2.0")
sepallengthGreaterThanOrEqualTo6 = iris.where("SepalLengthCM >= 6.0")
petalwidthLessThanOrEqualTo1 = iris.where("PetalWidthCM <= 1")
petalwidthInrange = iris.where("inRange(PetalWidthCM, 0, 1)")
```

### String filters

Deephaven stores strings as [Java strings](https://docs.oracle.com/en/java/javase/17/docs//api/java.base/java/lang/String.html). Any method you can call on a Java string can filter string data. Methods such as [`startsWith`](https://docs.oracle.com/en/java/javase/17/docs//api/java.base/java/lang/String.html#startsWith(java.lang.String)), [`endswith`](https://docs.oracle.com/en/java/javase/17/docs//api/java.base/java/lang/String.html#endsWith(java.lang.String)), [`contains`](https://docs.oracle.com/en/java/javase/17/docs//api/java.base/java/lang/String.html#contains(java.lang.CharSequence)), and [`matches`](https://docs.oracle.com/en/java/javase/17/docs//api/java.base/java/lang/String.html#matches(java.lang.String)) are particularly useful.

The following code block applies these methods to filter the `iris` table on its String column, `Class`:

```groovy test-set=1 order=classStartsWith,classEndsWith,classContains,classMatches
classStartsWith = iris.where("Class.startsWith(`Iris`)")
classEndsWith = iris.where("Class.endsWith(`setosa`)")
classContains = iris.where("Class.contains(`vir`)")
classMatches = iris.where("Class.matches(`Iris-versicolor`)")
```

### Regular expression (regex) filters

Regular expression filtering is another type of string filtering that uses [regular expressions](https://en.wikipedia.org/wiki/Regular_expression) to match patterns in string data. Deephaven's [`FilterPattern`](https://docs.deephaven.io/core/javadoc/io/deephaven/api/filter/FilterPattern.html) class enables the use of regex in filtering operations.

When performing regex filtering with `FilterPattern`, you apply a specific pattern mode. Two pattern modes exist:

- `FIND` looks for a subsequence match in the string.
- `MATCHES` matches an entire string against the given pattern.

The following code block performs regular expression filtering on the `iris` table with both pattern modes. The first finds strings that are eleven characters long, whereas the second looks for the subsequence `virginica` in the `Class` column:

```groovy test-set=1 order=irisElevenChars,irisVirginica
import io.deephaven.api.filter.FilterPattern
import java.util.regex.Pattern

filterElevenChars = FilterPattern.of(ColumnName.of("Class"), Pattern.compile("..........."), FilterPattern.Mode.MATCHES, false)
filterRegexMatch = FilterPattern.of(ColumnName.of("Class"), Pattern.compile("virginica"), FilterPattern.Mode.FIND, false)

irisElevenChars = iris.where(filterElevenChars)
irisVirginica = iris.where(filterRegexMatch)
```

## Combine filters

You can combine multiple match and/or conditional statements to filter data in a table. These combinations can be either conjunctive or disjunctive.

### Conjunctive

Conjunctive filtering returns a table where _all_ conditional filters in a [`where`](../reference/table-operations/filter/where.md) clause return `true`. Achieve conjunctive filtering by passing multiple filters into [`where`](../reference/table-operations/filter/where.md) via multiple parameters, or by using the `&&` operator in a single filter.

The following example applies a conjunctive filter to the `iris` table to produce a new table of only Iris setosa flowers with a petal length in a specific range.

```groovy test-set=1 order=conjunctiveFilteredIris
conjunctiveFilteredIris = iris.where("Class in `Iris-setosa`", "PetalLengthCM >= 1.3 && PetalLengthCM <= 1.6")
```

### Disjunctive

Disjunctive filtering returns a table where _one or more_ of the statements return `true`. You can accomplish this by using the `||` operator in a [`where`](../reference/table-operations/filter/where.md) statement, or by using [`Filter.or`](https://docs.deephaven.io/core/javadoc/io/deephaven/api/filter/Filter.html#or(io.deephaven.api.filter.Filter...)) to combine multiple filters.

In the following example, two filters work disjunctively to return a new table where the petal length is greater than 1.9 cm or less than 1.3 cm.

```groovy test-set=1 order=orFilteredIris,irisFilterOr
orFilteredIris = iris.where("PetalLengthCM > 1.9 || PetalWidthCM < 1.3")
irisFilterOr = iris.where(Filter.or(Filter.from("PetalLengthCM > 1.9", "PetalWidthCM < 1.3")))
```

## Filter one table based on another

The [`whereIn`](../reference/table-operations/filter/where-in.md) and [`whereNotIn`](../reference/table-operations/filter/where-not-in.md) methods filter one table based on another table. Deephaven evaluates these two methods whenever either table changes, whereas it only evaluates [`where`](../reference/table-operations/filter/where.md) when the filtered table ticks. ​The following example uses [`whereIn`](../reference/table-operations/filter/where-in.md) and [`whereNotIn`](../reference/table-operations/filter/where-not-in.md) to find Iris virginica sepal widths that match and do not match Iris versicolor sepal widths:

```groovy test-set=1 order=virginica,versicolor,virginicaMatchingPetalWidths,virginicaNonMatchingPetalWidths
virginica = iris.where("Class in `Iris-virginica`")
versicolor = iris.where("Class in `Iris-versicolor`")
virginicaMatchingPetalWidths = virginica.whereIn(versicolor, "PetalWidthCM")
virginicaNonMatchingPetalWidths = virginica.whereNotIn(versicolor, "PetalWidthCM")
```

> [!CAUTION]
> [`whereIn`](../reference/table-operations/filter/where-in.md) and [`whereNotIn`](../reference/table-operations/filter/where-not-in.md) are inefficient if the filter table updates frequently.

> [!TIP]
> Unlike [`naturalJoin`](../reference/table-operations/join/natural-join.md), [`whereIn`](../reference/table-operations/filter/where-in.md) works when more than one value in the right table matches values in the left table. This is true of [`join`](../reference/table-operations/join/join.md) as well, but [`whereIn`](../reference/table-operations/filter/where.md) returns matching rows faster than [`join`](../reference/table-operations/join/join.md).
>
> [`whereIn`](../reference/table-operations/filter/where-in.md) only provides filtering and does not allow adding columns from the right table. You may want to use [`whereIn`](../reference/table-operations/filter/where-in.md) to filter and then [`join`](../reference/table-operations/join/join.md) to add columns from the right table. This provides similar performance to [`naturalJoin`](../reference/table-operations/join/natural-join.md) while still allowing matches from the right table.

## Filter by row position

Filtering by row position removes unwanted data at the beginning, middle, or end of a table. You can specify row positions as absolute values or as a percentage of the total table size.

The following example uses [`head`](../reference/table-operations/filter/head.md), [`tail`](../reference/table-operations/filter/tail.md), and [`slice`](../reference/table-operations/filter/slice.md) to keep only the first, middle, and last 10 rows of `iris`, respectively.

```groovy test-set=1 order=irisHead,irisSlice,irisTail
irisHead = iris.head(10)
irisSlice = iris.slice(70, 80)
irisTail = iris.tail(10)
```

The following example uses [`headPct`](../reference/table-operations/filter/head-pct.md), [`slicePct`](../reference/table-operations/filter/slice-pct.md), and [`tailPct`](../reference/table-operations/filter/tail-pct.md) to keep only 10% of the rows at the top, middle, and end of the table, respectively.

```groovy test-set=1 order=irisHeadPct,irisSlicePct,irisTailPct
irisHeadPct = iris.headPct(0.1)
irisSlicePct = iris.slicePct(0.45, 0.55)
irisTailPct = iris.tailPct(0.1)
```

### Incremental Release Filter

The [IncrementalReleaseFilter](https://docs.deephaven.io/core/javadoc/io/deephaven/engine/table/impl/select/IncrementalReleaseFilter.html), available from Java and Groovy, converts a static or add-only table into a ticking table that parcels out rows over time. This is useful to simulate ticking data for development or to limit the number of rows that a complex query processes at one time. The incremental release filter takes two parameters:

- The initial number of rows to present in the resulting table.
- The number of rows to release at the beginning of each update graph cycle.

```groovy order=iris_incremental
import io.deephaven.csv.CsvTools
import io.deephaven.engine.table.impl.select.IncrementalReleaseFilter

iris = CsvTools.readCsv("https://media.githubusercontent.com/media/deephaven/examples/main/Iris/csv/iris.csv")
// release 10 rows initially, then release 5 rows at the beginning of each update graph cycle
iris_incremental = iris.where(new IncrementalReleaseFilter(10, 5))
```

## Related documentation

- [Filters in query strings](./filters.md)
- [Formulas in query strings](./formulas.md)
- [Operators in query strings](./operators.md)
- [equals](../reference/query-language/match-filters/equals.md)
- [`head`](../reference/table-operations/filter/head.md)
- [`headPct`](../reference/table-operations/filter/head-pct.md)
- [`icase in`](../reference/query-language/match-filters/icase-in.md)
- [`icase not in`](../reference/query-language/match-filters/icase-not-in.md)
- [`in`](../reference/query-language/match-filters/in.md)
- [`join`](../reference/table-operations/join/join.md)
- [`naturalJoin`](../reference/table-operations/join/natural-join.md)
- [`not in`](../reference/query-language/match-filters/not-in.md)
- [`slice`](../reference/table-operations/filter/slice.md)
- [`tail`](../reference/table-operations/filter/tail.md)
- [`tailPct`](../reference/table-operations/filter/tail-pct.md)
- [`where`](../reference/table-operations/filter/where.md)
- [`whereIn`](../reference/table-operations/filter/where-in.md)
