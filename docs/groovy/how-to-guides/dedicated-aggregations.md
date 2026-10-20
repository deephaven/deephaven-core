---
title: Single aggregation
---

This guide shows you how to compute summary information on groups of data using single (dedicated) aggregations.

Often when working with data, you want to break the data into subgroups and then perform calculations on the grouped data. For example, a multinational corporation might want to know its average employee salary by country, or a teacher might want to calculate grade information for groups of students or in certain subject areas.

The process of breaking a table into subgroups and then performing a single type of calculation on each subgroup is known as "single aggregation" or "dedicated aggregation." "Aggregation" refers to creating a summary of the data within each group. "Single" or "dedicated" means that only one type of calculation runs at a time.

Deephaven provides many dedicated aggregations, such as [`maxBy`](../reference/table-operations/group-and-aggregate/maxBy.md) and [`minBy`](../reference/table-operations/group-and-aggregate/minBy.md). These aggregations are good options when you need only one type of aggregation. If you need more than one type of aggregation or have a custom aggregation, [multi-aggregation](./combined-aggregations.md), also called combined aggregation, is a more efficient and more flexible solution.

## Syntax

The general syntax is:

```groovy skip-test
result = source.DEDICATED_AGG(groupByColumns...)
```

Replace `DEDICATED_AGG` with one of the aggregators listed in [Single aggregators](#single-aggregators). The `groupByColumns` parameter determines the column(s) by which to group data:

- Passing no grouping columns (for example, `source.lastBy()` or `source.countBy("Count")`) uses the whole table as a single group.
- `"X"` outputs the desired value for each group in column `X`.
- `"X", "Y"` outputs the desired value for each group designated from the `X` and `Y` columns.

Some aggregators take a required argument before the grouping columns:

- [`headBy`](../reference/table-operations/group-and-aggregate/headBy.md) and [`tailBy`](../reference/table-operations/group-and-aggregate/tailBy.md) take the number of rows.
- [`countBy`](../reference/table-operations/group-and-aggregate/countBy.md) takes the name of the count column.
- [`wavgBy`](../reference/table-operations/group-and-aggregate/wavgBy.md) and [`wsumBy`](../reference/table-operations/group-and-aggregate/wsumBy.md) take the weight column.

## Single aggregators

Each dedicated aggregator performs one calculation at a time:

- [`absSumBy`](../reference/table-operations/group-and-aggregate/absSumBy.md) - Sum of absolute values of each group.
- [`avgBy`](../reference/table-operations/group-and-aggregate/avgBy.md) - Average (mean) of each group.
- [`countBy`](../reference/table-operations/group-and-aggregate/countBy.md) - Number of rows in each group.
- [`firstBy`](../reference/table-operations/group-and-aggregate/firstBy.md) - First row of each group.
- [`groupBy`](../reference/table-operations/group-and-aggregate/groupBy.md) - Group column content into [vectors](../conceptual/arrays-vs-vectors.md), one row per group.
- [`headBy`](../reference/table-operations/group-and-aggregate/headBy.md) - First `n` rows of each group.
- [`lastBy`](../reference/table-operations/group-and-aggregate/lastBy.md) - Last row of each group.
- [`maxBy`](../reference/table-operations/group-and-aggregate/maxBy.md) - Maximum value of each group.
- [`medianBy`](../reference/table-operations/group-and-aggregate/medianBy.md) - Median of each group.
- [`minBy`](../reference/table-operations/group-and-aggregate/minBy.md) - Minimum value of each group.
- [`stdBy`](../reference/table-operations/group-and-aggregate/stdBy.md) - Sample standard deviation of each group.
- [`sumBy`](../reference/table-operations/group-and-aggregate/sumBy.md) - Sum of each group.
- [`tailBy`](../reference/table-operations/group-and-aggregate/tailBy.md) - Last `n` rows of each group.
- [`varBy`](../reference/table-operations/group-and-aggregate/varBy.md) - Sample variance of each group.
- [`wavgBy`](../reference/table-operations/group-and-aggregate/wavgBy.md) - Weighted average of each group.
- [`wsumBy`](../reference/table-operations/group-and-aggregate/wsumBy.md) - Weighted sum of each group.

The examples on this page cover every aggregator in this list except [`absSumBy`](../reference/table-operations/group-and-aggregate/absSumBy.md), [`groupBy`](../reference/table-operations/group-and-aggregate/groupBy.md), [`wavgBy`](../reference/table-operations/group-and-aggregate/wavgBy.md), and [`wsumBy`](../reference/table-operations/group-and-aggregate/wsumBy.md). For those four, see the examples on their reference pages.

## Select and count rows in each group

In the following examples, we have test results in various subjects for some students. We want to summarize each student's results.

```groovy test-set=1
source = newTable(
    stringCol("Name", "James", "James", "James", "Lauren", "Lauren", "Lauren", "Zoey", "Zoey", "Zoey"),
    stringCol("Subject", "Math", "Science", "Art", "Math", "Science", "Art", "Math", "Science", "Art"),
    intCol("Number", 95, 100, 90, 72, 78, 92, 100, 98, 96),
)
```

### `firstBy` and `lastBy`

In this example, we want to know the first and the last test results for each student. To achieve this, we can use [`firstBy`](../reference/table-operations/group-and-aggregate/firstBy.md) to return the first test value and [`lastBy`](../reference/table-operations/group-and-aggregate/lastBy.md) to return the last test value. The example groups the results by `Name`.

```groovy test-set=1 order=first,last
first = source.firstBy("Name")
last = source.lastBy("Name")
```

For how [`lastBy`](../reference/table-operations/group-and-aggregate/lastBy.md) decides which row is last, and how to get the last row after sorting by a column, see [Find the last row for each key](#find-the-last-row-for-each-key-with-lastby).

### `headBy` and `tailBy`

In this example, we want to know the first two and the last two test results for each student. To achieve this, we can use [`headBy`](../reference/table-operations/group-and-aggregate/headBy.md) to return the first two test values and [`tailBy`](../reference/table-operations/group-and-aggregate/tailBy.md) to return the last two test values. The example groups the results by `Name`.

```groovy test-set=1 order=head,tail
head = source.headBy(2, "Name")
tail = source.tailBy(2, "Name")
```

### `countBy`

In this example, we want to know the number of tests each student completed. [`countBy`](../reference/table-operations/group-and-aggregate/countBy.md) returns the number of rows in the table as grouped by `Name` and stores that in a new column, `NumTests`.

```groovy test-set=1
count = source.countBy("NumTests", "Name")
```

## Summary statistics aggregators

The following examples use the `source` table of student test results created in [Select and count rows in each group](#select-and-count-rows-in-each-group). It has a `Name` column, a string `Subject` column, and an integer `Number` column of scores. Because of the type restrictions below, the examples remove the string column `Subject` with [`dropColumns`](../reference/table-operations/select/drop-columns.md), so each result contains only score statistics.

> [!CAUTION]
> [`sumBy`](../reference/table-operations/group-and-aggregate/sumBy.md), [`avgBy`](../reference/table-operations/group-and-aggregate/avgBy.md), [`stdBy`](../reference/table-operations/group-and-aggregate/stdBy.md), and [`varBy`](../reference/table-operations/group-and-aggregate/varBy.md) throw an error on a column of an unsupported type, such as a string column. These aggregators accept string columns:
>
> - [`medianBy`](../reference/table-operations/group-and-aggregate/medianBy.md) returns the middle value in sort order.
> - [`minBy`](../reference/table-operations/group-and-aggregate/minBy.md) returns the smallest value in sort order.
> - [`maxBy`](../reference/table-operations/group-and-aggregate/maxBy.md) returns the largest value in sort order.
>
> In sort order, uppercase letters come before lowercase letters.

### `sumBy`

In this example, [`sumBy`](../reference/table-operations/group-and-aggregate/sumBy.md) calculates the sum of test scores for each `Name`.

```groovy test-set=1
sum = source.dropColumns("Subject").sumBy("Name")
```

### `avgBy`

In this example, [`avgBy`](../reference/table-operations/group-and-aggregate/avgBy.md) calculates the average (mean) of test scores for each `Name`.

```groovy test-set=1
mean = source.dropColumns("Subject").avgBy("Name")
```

### `stdBy`

In this example, [`stdBy`](../reference/table-operations/group-and-aggregate/stdBy.md) calculates the sample standard deviation of test scores for each `Name`.

```groovy test-set=1
stdDev = source.dropColumns("Subject").stdBy("Name")
```

### `varBy`

In this example, [`varBy`](../reference/table-operations/group-and-aggregate/varBy.md) calculates the sample variance of test scores for each `Name`.

```groovy test-set=1
var = source.dropColumns("Subject").varBy("Name")
```

### `medianBy`

In this example, [`medianBy`](../reference/table-operations/group-and-aggregate/medianBy.md) calculates the median of test scores for each `Name`.

```groovy test-set=1
median = source.dropColumns("Subject").medianBy("Name")
```

### `minBy`

In this example, [`minBy`](../reference/table-operations/group-and-aggregate/minBy.md) calculates the minimum of test scores for each `Name`.

```groovy test-set=1
minimum = source.dropColumns("Subject").minBy("Name")
```

### `maxBy`

In this example, [`maxBy`](../reference/table-operations/group-and-aggregate/maxBy.md) calculates the maximum of test scores for each `Name`.

```groovy test-set=1
maximum = source.dropColumns("Subject").maxBy("Name")
```

## Find the last row for each key with `lastBy`

The [`lastBy`](../reference/table-operations/group-and-aggregate/lastBy.md) operation creates a new table that contains the last row of the input table for each key. The key columns are the grouping columns you pass to `lastBy`. A key is one unique value or combination of values in those columns. [`firstBy`](../reference/table-operations/group-and-aggregate/firstBy.md) follows the same rules but returns the first row for each key.

The "last" row is the row closest to the bottom of the table. It depends only on the order of rows in the source table. It does not depend on values in the data, such as timestamps or sequence numbers. In a live-updating table, it is not necessarily the row that most recently arrived.

When you pass no key columns, [`lastBy`](../reference/table-operations/group-and-aggregate/lastBy.md) returns the last row in the table:

```groovy order=t2,t
t = emptyTable(10).update("MyCol = ii + 1")
t2 = t.lastBy()
```

When you pass key columns (such as `MyKey` in the example below), [`lastBy`](../reference/table-operations/group-and-aggregate/lastBy.md) returns the last row for each key:

```groovy order=t2,t
t = emptyTable(10).update("MyKey = Long.toString(10 + ii % 5, 36).toUpperCase()", "MyCol = ii")
t2 = t.lastBy("MyKey")
```

You can use multiple key columns:

```groovy order=t2,t
t = emptyTable(10).update(
        "MyKey = Long.toString(10 + ii % 5, 36).toUpperCase()",
        "MySecondKey = ii % 2",
        "MyCol = ii",
)
t2 = t.lastBy("MyKey", "MySecondKey")
```

You often use [`lastBy`](../reference/table-operations/group-and-aggregate/lastBy.md) with time series data to get a table that shows the current state of each key. When data is out of order, sort it before applying `lastBy`. For example, the data below is naturally ordered by `Timestamp1`. To find the latest rows based on `Timestamp2`, [`sort`](../reference/table-operations/sort/sort.md) the data before running `lastBy`:

```groovy test-set=2 order=t2,tSorted,t
startTime = now()

t = emptyTable(100).update(
        "Timestamp1 = startTime + SECOND * ii",
        "Timestamp2 = startTime + SECOND * randomInt(0, 100)",
        "MyKey = Long.toString(10 + ii % 5, 36).toUpperCase()",
        "MyCol = ii",
)

tSorted = t.sort("Timestamp2")
t2 = tSorted.lastBy("MyKey")
```

If the query doesn't use the sorted table anywhere else, use [`AggSortedLast`](../reference/table-operations/group-and-aggregate/AggSortedLast.md) instead of sorting first. It avoids sorting the whole table:

- It splits the rows into groups by the key columns.
- Within each group, it finds the row with the largest sort-column value.
- It doesn't build a sorted copy of the whole table, so it typically uses less memory and processing than [`sort`](../reference/table-operations/sort/sort.md) followed by [`lastBy`](../reference/table-operations/group-and-aggregate/lastBy.md).

To use it, pass [`AggSortedLast`](../reference/table-operations/group-and-aggregate/AggSortedLast.md) to [`aggBy`](../reference/table-operations/group-and-aggregate/aggBy.md) with the sort column as its first argument, followed by the columns to return. The example below reuses `t` from the previous example and picks the same row for each key as that example's `t2`, without creating an intermediate sorted table. The key column comes first and rows can appear in a different order. Unlike [`lastBy`](../reference/table-operations/group-and-aggregate/lastBy.md), it returns only the key columns and the columns you list after the sort column, so the example lists `Timestamp2` too.

```groovy test-set=2 order=t3
// last row by MyKey, after sorting by Timestamp2
t3 = t.aggBy(AggSortedLast("Timestamp2", "Timestamp1", "Timestamp2", "MyCol"), "MyKey")
```

## Aggregate columns in the UI

You can also aggregate a column in the UI with the **Aggregate Columns** feature. Unlike the programmatic examples above, it doesn't create a new table with one row per group. Instead, it adds a row at the top or bottom of the table that applies the operation you choose to each column. Use it to quickly see the sum, average, or another aggregate of a column.

In the example below, we add an Average row and a Minimum row to the top of the table:

![A user aggregates columns in the UI with the **Aggregate Columns** feature](../assets/how-to/ui/aggregate_columns.gif)

You can reorder, edit, or delete these aggregations in the **Aggregate Columns** dialog.

## Related documentation

- [Multi-aggregation](./combined-aggregations.md)
- [`absSumBy`](../reference/table-operations/group-and-aggregate/absSumBy.md)
- [`aggBy`](../reference/table-operations/group-and-aggregate/aggBy.md)
- [`AggSortedLast`](../reference/table-operations/group-and-aggregate/AggSortedLast.md)
- [`avgBy`](../reference/table-operations/group-and-aggregate/avgBy.md)
- [`countBy`](../reference/table-operations/group-and-aggregate/countBy.md)
- [`dropColumns`](../reference/table-operations/select/drop-columns.md)
- [`firstBy`](../reference/table-operations/group-and-aggregate/firstBy.md)
- [`groupBy`](../reference/table-operations/group-and-aggregate/groupBy.md)
- [`headBy`](../reference/table-operations/group-and-aggregate/headBy.md)
- [`lastBy`](../reference/table-operations/group-and-aggregate/lastBy.md)
- [`maxBy`](../reference/table-operations/group-and-aggregate/maxBy.md)
- [`medianBy`](../reference/table-operations/group-and-aggregate/medianBy.md)
- [`minBy`](../reference/table-operations/group-and-aggregate/minBy.md)
- [`sort`](../reference/table-operations/sort/sort.md)
- [`stdBy`](../reference/table-operations/group-and-aggregate/stdBy.md)
- [`sumBy`](../reference/table-operations/group-and-aggregate/sumBy.md)
- [`tailBy`](../reference/table-operations/group-and-aggregate/tailBy.md)
- [`varBy`](../reference/table-operations/group-and-aggregate/varBy.md)
- [`wavgBy`](../reference/table-operations/group-and-aggregate/wavgBy.md)
- [`wsumBy`](../reference/table-operations/group-and-aggregate/wsumBy.md)
