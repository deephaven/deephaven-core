---
title: Sort table data
---

Sorting is a common operation in data analysis, and Deephaven makes it easy to sort data in a variety of ways. This guide shows how to sort table data programmatically by one or more columns and in different directions, and explains the order in which Deephaven sorts values. It also covers [`restrict_sort_to`](../reference/table-operations/sort/restrict-sort.md), which limits the columns users can sort a table on.

> [!TIP]
> You can also sort columns by right-clicking the column header in the UI.

## Sort by one column

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

Given a column name to order the data by, [`sort`](../reference/table-operations/sort/sort.md) returns a new table with the data sorted in ascending (A-Z) order, and [`sort_descending`](../reference/table-operations/sort/sort-descending.md) returns a new table with the data sorted in descending (Z-A) order. In `result_sort`, the three `A` rows keep their original order (`red`, `orange`, `pink`) because the sort is [stable](#sort-order-rules).

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
- **Arrays and vectors:** Deephaven sorts [array and vector columns](./work-with-arrays.md), such as the output of [`group_by`](../reference/table-operations/group-and-aggregate/groupBy.md), lexicographically. It compares the first elements, then the second, and so on. An array that matches the start of a longer array sorts first. Deephaven registers comparators by default for columns whose type is exactly one of these array types: `byte[]`, `short[]`, `int[]`, `long[]`, `char[]`, `float[]`, `double[]`, `String[]`, `BigInteger[]`, `BigDecimal[]`, `Comparable[]`, `Instant[]`, `ZonedDateTime[]`, `LocalDate[]`, `LocalDateTime[]`, `LocalTime[]`, `Date[]`, and `Duration[]`. Sorting a column of any other array type, such as `Integer[]`, raises an error that wraps a `NotSortableColumnException`, even when the array's elements are `Comparable`.

To see the null and `NaN` rules in action, let's sort a column that holds a null, a `NaN`, and two ordinary values in both directions:

```python test-set=1 order=result_rules_asc,result_rules_desc
from deephaven.column import double_col
from deephaven.constants import NULL_DOUBLE, NAN_DOUBLE

values = new_table([double_col("X", [2.0, NULL_DOUBLE, NAN_DOUBLE, -1.0])])

result_rules_asc = values.sort(order_by="X")
result_rules_desc = values.sort_descending(order_by="X")
```

In `result_rules_asc`, the order is null, `-1.0`, `2.0`, `NaN`. In `result_rules_desc`, the order is reversed: `NaN`, `2.0`, `-1.0`, null.

## Sort by multiple columns

Both [`sort`](../reference/table-operations/sort/sort.md) and [`sort_descending`](../reference/table-operations/sort/sort-descending.md) can sort by multiple columns at once. Let's sort the `source` table from [Sort by one column](#sort-by-one-column) by the `Letter` column, then use the `Number` column to order rows that share a `Letter` value. [`sort_descending`](../reference/table-operations/sort/sort-descending.md) applies descending order to both columns.

```python test-set=1 order=result_sort,result_sort_desc
result_sort = source.sort(order_by=["Letter", "Number"])
result_sort_desc = source.sort_descending(order_by=["Letter", "Number"])
```

## Sort columns in different directions

Pass a list of [`SortDirection`](/core/pydoc/code/deephaven.html#deephaven.SortDirection) values to the `order` argument of [`sort`](../reference/table-operations/sort/sort.md) to sort each column in its own direction. For example, let's sort by `Letter` in ascending order, then by `Number` in descending order:

```python test-set=1 order=result
from deephaven import SortDirection

asc = SortDirection.ASCENDING
desc = SortDirection.DESCENDING

result = source.sort(order_by=["Letter", "Number"], order=[asc, desc])
```

You can get the same result by chaining [`sort`](../reference/table-operations/sort/sort.md) and [`sort_descending`](../reference/table-operations/sort/sort-descending.md), but a list of directions is simpler. In a chain, the last sort becomes the primary sort key, so the sorts go in reverse priority order. This works because each sort is [stable](#sort-order-rules): rows that tie on the last sort's column keep the order the earlier sort gave them. Here, `Number` is sorted first and `Letter` last:

```python test-set=1 order=result
result = source.sort_descending(order_by="Number").sort(order_by="Letter")
```

## Restrict which columns can be sorted

The [`restrict_sort_to`](../reference/table-operations/sort/restrict-sort.md) method restricts which columns users can sort the table on, both in the UI and in code. This is useful if you want to prevent yourself or other users from accidentally performing expensive [sort](../reference/table-operations/sort/sort.md) operations as you interact with tables in the UI. For example, let's restrict sorting to the `Letter` column:

```python test-set=1 order=table
table = source.restrict_sort_to(cols="Letter")
```

We can still sort by `Letter`:

```python test-set=1 order=t_sorted_letter
t_sorted_letter = table.sort(order_by="Letter")
```

However, sorting by `Number` raises an error:

```python test-set=1 skip-test
t_sorted = table.sort(order_by="Number")
```

The call raises a `DHError` whose message names the column you tried to sort on.

## Related documentation

- [`sort`](../reference/table-operations/sort/sort.md)
- [`sort_descending`](../reference/table-operations/sort/sort-descending.md)
- [`restrict_sort_to`](../reference/table-operations/sort/restrict-sort.md)
- [`SortDirection`](/core/pydoc/code/deephaven.html#deephaven.SortDirection)
- [`reverse`](../reference/table-operations/sort/reverse.md)
- [Null, infinity, and NaN values](./null-inf-nan.md)
- [Work with arrays](./work-with-arrays.md)
