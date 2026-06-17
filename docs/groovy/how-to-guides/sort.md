---
title: Sort table data
sidebar_label: Sort
---

Sorting is a common operation in data analysis, and Deephaven makes it easy to sort data in a variety of ways. This guide will show you how to sort table data programmatically.

> [!TIP]
> You can also sort columns by right-clicking the column header in the UI.

## `sort` and `sortDescending`

You can sort table data programmatically by using the [`sort`](../reference/table-operations/sort/sort.md) and [`sortDescending`](../reference/table-operations/sort/sort-descending.md) methods:

```groovy test-set=1 order=resultSort,resultSortDesc
source = newTable(stringCol("Letter", "A", "B", "A", "B", "B", "A"), intCol("Number", 6, 6, 1, 3, 4, 4), stringCol("Color", "red", "blue", "orange", "purple", "yellow", "pink"))

resultSort = source.sort("Letter")
resultSortDesc = source.sortDescending("Letter")
```

Given a column name to order the data by, [`sort`](../reference/table-operations/sort/sort.md) returns a new table with the data sorted in ascending (A-Z) order, and [`sortDescending`](../reference/table-operations/sort/sort-descending.md) returns a new table with the data sorted in descending (Z-A) order.

Deephaven typically sorts values by their _natural order_ with null values first. Deephaven's sort is _stable_, meaning that the order of two equal elements is not changed. For floating point values, nulls are first, followed by normal values, and finally, `NaN` values come last. Both positive zero and negative zero are treated as identical values, preserving the original order of two rows with a zero value. Arrays of primitives, Strings, BigDecimal, BigInteger, and [Comparable](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/lang/Comparable.html) values are sorted lexicographically.

## Sorting by multiple columns

Both `sort` and `sortDescending` can sort multiple columns at once. For example, here we will sort the table from the previous example by our `Letter` column, then the `Number` column.

```groovy test-set=1 order=resultSort,resultSortDesc
resultSort = source.sort("Letter", "Number")
resultSortDesc = source.sortDescending("Letter", "Number")
```

## Complex sorts

The `sort` method can be used to sort multiple columns in different directions. For example, we can sort by `Letter` in ascending order, then by `Number` in descending order.

```groovy test-set=1 order=result
sortColumns = [
    SortColumn.asc(ColumnName.of("Letter")),
    SortColumn.desc(ColumnName.of("Number"))
]

result = source.sort(sortColumns)
```

This is simpler than invoking both methods to accomplish the same result:

```groovy test-set=1 order=result
result = source.sort("Letter").sortDescending("Number")
```

## `restrictSortTo`

The `restrictSortTo` method allows you to restrict the columns that can be sorted via the UI. This is useful if you want to prevent yourself or other users from accidentally performing expensive sort operations as you interact with tables in the UI. For example, we can restrict sorting to the `Letter` column:

```groovy test-set=1 order=table
table = source.restrictSortTo("Letter")
```

Now, we can still sort by `Letter`, but attempting to sort by `Number` will result in an error:

```groovy test-set=1 skip-test
tSorted = table.sort("Number")
```

## Related documentation

- [`sort`](../reference/table-operations/sort/sort.md)
- [`sortDescending`](../reference/table-operations/sort/sort-descending.md)
- [`reverse`](../reference/table-operations/sort/reverse.md)
