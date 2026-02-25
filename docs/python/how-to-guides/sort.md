---
title: Sort table data
sidebar_label: Sort
---

Sorting is a common operation in data analysis, and Deephaven makes it easy to sort data in a variety of ways. This guide will show you how to sort table data programmatically.

> [!TIP]
> You can also sort columns by right-clicking the column header in the UI.

## `sort` and `sort_descending`

You can sort table data programmatically by using the [`sort`](../reference/table-operations/sort/sort.md) and [`sort_descending`](../reference/table-operations/sort/sort-descending.md) methods:

```python test-set=1 order=result_sort,result_sort_desc
from deephaven import new_table
from deephaven.column import string_col, int_col

source = new_table(
    [
        string_col("Letter", ["A", "B", "A", "B", "B", "A"]),
        int_col("Number", [6, 6, 1, 3, 4, 4]),
        string_col("Color", ["red", "blue", "orange", "purple", "yellow", "pink"]),
    ]
)

result_sort = source.sort(order_by=["Letter"])
result_sort_desc = source.sort_descending(order_by=["Letter"])
```

Given a column name to order the data by, [`sort`](../reference/table-operations/sort/sort.md) returns a new table with the data sorted in ascending (A-Z) order, and [`sort_descending`](../reference/table-operations/sort/sort-descending.md) returns a new table with the data sorted in descending (Z-A) order.

Deephaven typically sorts values by their _natural order_ with null values first. Deephaven's sort is _stable_, meaning that the order of two equal elements is not changed. For floating point values, nulls are first, followed by normal values, and finally, `NaN` values come last. Both positive zero and negative zero are treated as identical values, preserving the original order of two rows with a zero value. Arrays of primitives, Strings, BigDecimal, BigInteger, and [Comparable](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/lang/Comparable.html) values are sorted lexicographically.

## Sorting by multiple columns

Both `sort` and `sort_descending` can sort multiple columns at once. For example, here we will sort the table from the previous example by our `Letter` column, then the `Number` column.

```python test-set=1 order=result_sort,result_sort_desc
result_sort = source.sort(order_by=["Letter", "Number"])
result_sort_desc = source.sort_descending(order_by=["Letter", "Number"])
```

## Complex sorts

The `sort` method can be used to sort multiple columns in different directions. For example, we can sort by `Letter` in ascending order, then by `Number` in descending order.

```python test-set=1 order=result
from deephaven import SortDirection

asc = SortDirection.ASCENDING
desc = SortDirection.DESCENDING

result = source.sort(order_by=["Letter", "Number"], order=[asc, desc])
```

This is simpler than invoking both methods to accomplish the same result:

```python test-set=1 order=result
result = source.sort(order_by="Letter").sort_descending(order_by="Number")
```

## `restrict_sort_to`

The `restrict_sort_to` method allows you to restrict the columns that can be sorted via the UI. This is useful if you want to prevent yourself or other users from accidentally performing expensive sort operations as you interact with tables in the UI. For example, we can restrict sorting to the `Letter` column:

```python test-set=1 order=table
table = source.restrict_sort_to(cols="Letter")
```

Now, we can still sort by `Letter`, but attempting to sort by `Number` will result in an error:

```python test-set=1 skip-test
t_sorted = table.sort(order_by="Number")
```

## Related documentation

- [`sort`](../reference/table-operations/sort/sort.md)
- [`sort_descending`](../reference/table-operations/sort/sort-descending.md)
- [`reverse`](../reference/table-operations/sort/reverse.md)
