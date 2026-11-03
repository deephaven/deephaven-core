---
title: Sort table data
---

Sorting is a common operation in data analysis, and Deephaven makes it easy to sort data in a variety of ways. This guide shows how to sort table data programmatically by one or more columns and in different directions, and explains the order in which Deephaven sorts values. It also covers [`restrictSortTo`](../reference/table-operations/sort/restrict-sort.md), which limits the columns users can sort a table on.

> [!TIP]
> You can also sort columns by right-clicking the column header in the UI.

## Sort by one column

You can sort table data programmatically by using the [`sort`](../reference/table-operations/sort/sort.md) and [`sortDescending`](../reference/table-operations/sort/sort-descending.md) methods:

```groovy test-set=1 order=resultSort,resultSortDesc
source = newTable(stringCol("Letter", "A", "B", "A", "B", "B", "A"), intCol("Number", 6, 6, 1, 3, 4, 4), stringCol("Color", "red", "blue", "orange", "purple", "yellow", "pink"))

resultSort = source.sort("Letter")
resultSortDesc = source.sortDescending("Letter")
```

Given a column name to order the data by, [`sort`](../reference/table-operations/sort/sort.md) returns a new table with the data sorted in ascending (A-Z) order, and [`sortDescending`](../reference/table-operations/sort/sort-descending.md) returns a new table with the data sorted in descending (Z-A) order. In `resultSort`, the three `A` rows keep their original order (`red`, `orange`, `pink`) because the sort is [stable](#sort-order-rules).

Sorting works the same way on live data: when the source table ticks, the sorted result updates and stays in order.

## Sort order rules

Deephaven sorts values by their _natural order_, which is the order the column's type defines:

- Numbers sort in numeric order.
- Date-times sort in chronological order.
- Strings sort character by character, and the comparison is case-sensitive, so uppercase letters sort before lowercase letters.
- Other [`Comparable`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/lang/Comparable.html) values sort by their own `compareTo` method.

Deephaven's [sort](../reference/table-operations/sort/sort.md) is also _stable_: rows with equal sort values keep their original relative order.

Natural order has special rules for nulls, floating-point values, and arrays:

- **Nulls:** In ascending order, [null values](./null-inf-nan.md) sort first. [Descending sorts](../reference/table-operations/sort/sort-descending.md) reverse this, so null values come last.
- **Floating-point values:** [`NaN`](./null-inf-nan.md) sorts after all other values in ascending order and first in descending order. Positive zero and negative zero are equal, so two rows whose values are `0.0` and `-0.0` keep their original order.
- **Arrays and vectors:** Deephaven sorts [array and vector columns](./work-with-arrays.md), such as the output of [`groupBy`](../reference/table-operations/group-and-aggregate/groupBy.md), lexicographically. It compares the first elements, then the second, and so on. An array that matches the start of a longer array sorts first. Deephaven registers comparators by default for columns whose type is exactly one of these array types: `byte[]`, `short[]`, `int[]`, `long[]`, `char[]`, `float[]`, `double[]`, `String[]`, `BigInteger[]`, `BigDecimal[]`, `Comparable[]`, `Instant[]`, `ZonedDateTime[]`, `LocalDate[]`, `LocalDateTime[]`, `LocalTime[]`, `Date[]`, and `Duration[]`. Sorting a column of any other array type, such as `Integer[]`, throws a `NotSortableColumnException`, even when the array's elements are `Comparable`.

To see the null and `NaN` rules in action, let's sort a column that holds a null, a `NaN`, and two ordinary values in both directions:

```groovy test-set=1 order=resultRulesAsc,resultRulesDesc
values = newTable(doubleCol("X", 2.0d, NULL_DOUBLE, NAN_DOUBLE, -1.0d))

resultRulesAsc = values.sort("X")
resultRulesDesc = values.sortDescending("X")
```

In `resultRulesAsc`, the order is null, `-1.0`, `2.0`, `NaN`. In `resultRulesDesc`, the order is reversed: `NaN`, `2.0`, `-1.0`, null.

## Sort by multiple columns

Both [`sort`](../reference/table-operations/sort/sort.md) and [`sortDescending`](../reference/table-operations/sort/sort-descending.md) can sort by multiple columns at once. Let's sort the `source` table from [Sort by one column](#sort-by-one-column) by the `Letter` column, then use the `Number` column to order rows that share a `Letter` value. [`sortDescending`](../reference/table-operations/sort/sort-descending.md) applies descending order to both columns.

```groovy test-set=1 order=resultSort,resultSortDesc
resultSort = source.sort("Letter", "Number")
resultSortDesc = source.sortDescending("Letter", "Number")
```

## Sort columns in different directions

To sort each column in its own direction, pass [`sort`](../reference/table-operations/sort/sort.md) a list of [`SortColumn`](/core/javadoc/io/deephaven/api/SortColumn.html) objects, one per column, in priority order:

- [`SortColumn.asc`](/core/javadoc/io/deephaven/api/SortColumn.html#asc(io.deephaven.api.ColumnName)) sorts a column in ascending order.
- [`SortColumn.desc`](/core/javadoc/io/deephaven/api/SortColumn.html#desc(io.deephaven.api.ColumnName)) sorts a column in descending order.

Both methods take a [`ColumnName`](/core/javadoc/io/deephaven/api/ColumnName.html), which [`ColumnName.of`](/core/javadoc/io/deephaven/api/ColumnName.html#of(java.lang.String)) creates from the column's name. Let's sort by `Letter` in ascending order, then by `Number` in descending order:

```groovy test-set=1 order=result
sortColumns = [
    SortColumn.asc(ColumnName.of("Letter")),
    SortColumn.desc(ColumnName.of("Number"))
]

result = source.sort(sortColumns)
```

You can get the same result by chaining [`sort`](../reference/table-operations/sort/sort.md) and [`sortDescending`](../reference/table-operations/sort/sort-descending.md), but a list of [`SortColumn`](/core/javadoc/io/deephaven/api/SortColumn.html) objects is simpler. In a chain, the last sort becomes the primary sort key, so the sorts go in reverse priority order. This works because each sort is [stable](#sort-order-rules): rows that tie on the last sort's column keep the order the earlier sort gave them. Here, `Number` is sorted first and `Letter` last:

```groovy test-set=1 order=result
result = source.sortDescending("Number").sort("Letter")
```

## Restrict which columns can be sorted

The [`restrictSortTo`](../reference/table-operations/sort/restrict-sort.md) method restricts which columns users can sort the table on, both in the UI and in code. This is useful if you want to prevent yourself or other users from accidentally performing expensive [sort](../reference/table-operations/sort/sort.md) operations as you interact with tables in the UI. For example, let's restrict sorting to the `Letter` column:

```groovy test-set=1 order=table
table = source.restrictSortTo("Letter")
```

We can still sort by `Letter`:

```groovy test-set=1 order=tSortedLetter
tSortedLetter = table.sort("Letter")
```

However, sorting by `Number` throws an exception:

```groovy test-set=1 skip-test
tSorted = table.sort("Number")
```

The call throws a `NotSortableException` whose message names the column you tried to sort on.

To remove the restriction, call [`clearSortingRestrictions`](/core/javadoc/io/deephaven/engine/table/GridAttributes.html#clearSortingRestrictions()) on the table. It returns a table that can be sorted on any column.

## Related documentation

- [`sort`](../reference/table-operations/sort/sort.md)
- [`sortDescending`](../reference/table-operations/sort/sort-descending.md)
- [`restrictSortTo`](../reference/table-operations/sort/restrict-sort.md)
- [`SortColumn`](/core/javadoc/io/deephaven/api/SortColumn.html)
- [`ColumnName`](/core/javadoc/io/deephaven/api/ColumnName.html)
- [`reverse`](../reference/table-operations/sort/reverse.md)
- [Null, infinity, and NaN values](./null-inf-nan.md)
- [Work with arrays](./work-with-arrays.md)
