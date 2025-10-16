---
title: Single aggregation
---

This guide will show you how to compute summary information on groups of data using dedicated data aggregations.

Often when working with data, you will want to break the data into subgroups and then perform calculations on the grouped data. For example, a large multi-national corporation may want to know their average employee salary by country, or a teacher might want to calculate grade information for groups of students or in certain subject areas.

The process of breaking a table into subgroups and then performing a single type of calculation on the subgroups is known as "dedicated aggregation." The term comes from most operations creating a summary of data within a group (aggregation) and from a single type of operation being computed at once (dedicated).

Deephaven provides many dedicated aggregations, such as [`maxBy`](../reference/table-operations/group-and-aggregate/maxBy.md) and [`minBy`](../reference/table-operations/group-and-aggregate/minBy.md). These aggregations are good options if only one type of aggregation is needed. If more than one type of aggregation is needed or if you have a custom aggregation, [combined aggregations](./combined-aggregations.md) are a more efficient and more flexible solution.

## Syntax

The general syntax follows:

```groovy skip-test
result = source.DEDICATED_AGG(columnNames)
```

The `columnNames` parameter determines the column(s) by which to group data.

- `DEDICATED_AGG` should be substituted with one of the chosen aggregations below
- `NULL` uses the whole table as a single group.
- `"X"` will output the desired value for each group in column `X`.
- `"X", "Y"` will output the desired value for each group designated from the `X` and `Y` columns.

## Single aggregators

Each dedicated aggregator performs one calculation at a time:

- [`absSumBy`](../reference/table-operations/group-and-aggregate/AbsSumBy.md) - Sum of absolute values of each group.
- [`avgBy`](../reference/table-operations/group-and-aggregate/avgBy.md) - Average (mean) of each group.
- [`countBy`](../reference/table-operations/group-and-aggregate/countBy.md) - Number of rows in each group.
- [`firstBy`](../reference/table-operations/group-and-aggregate/firstBy.md) - First row of each group.
- [`groupBy`](../reference/table-operations/group-and-aggregate/groupBy.md) - Group column content into vectors.
- [`headBy`](../reference/table-operations/group-and-aggregate/headBy.md) - First `n` rows of each group.
- [`lastBy`](../reference/table-operations/group-and-aggregate/lastBy.md) - Last row of each group.
- [`maxBy`](../reference/table-operations/group-and-aggregate/maxBy.md) - Maximum value of each group.
- [`medianBy`](../reference/table-operations/group-and-aggregate/medianBy.md) - Median of each group.
- [`minBy`](../reference/table-operations/group-and-aggregate/minBy.md) - Minimum value of each group.
- [`stdBy`](../reference/table-operations/group-and-aggregate/stdBy.md) - Sample standard deviation of each group.
- [`sumBy`](../reference/table-operations/group-and-aggregate/sumBy.md) - Sum of each group.
- [`tailBy`](../reference/table-operations/group-and-aggregate/tailBy.md) - Last `n` rows of each group.
- [`varBy`](../reference/table-operations/group-and-aggregate/varBy.md) - Sample variance of each group.
- [`weightedAvgBy`](../reference/table-operations/group-and-aggregate/weighted-sum-by.md) - Weighted average of each group.
- [`weightedSumBy`](../reference/table-operations/group-and-aggregate/weighted-sum-by.md) - Weighted sum of each group.

In the following examples, we have test results in various subjects for some students. We want to summarize this information to see if students perform better in one class or another.

```groovy test-set=1
source = newTable(
    stringCol("Name", "James", "James", "James", "Lauren", "Lauren", "Lauren", "Zoey", "Zoey", "Zoey"),
    stringCol("Subject", "Math", "Science", "Art", "Math", "Science", "Art", "Math", "Science", "Art"),
    intCol("Number", 95, 100, 90, 72, 78, 92, 100, 98, 96),
)
```

### `firstBy` and `lastBy`

In this example, we want to know the first and the last test results for each student. To achieve this, we can use [`firstBy`](../reference/table-operations/group-and-aggregate/firstBy.md) to return the first test value and [`lastBy`](../reference/table-operations/group-and-aggregate/lastBy.md) to return the last test value. The results are grouped by `Name`.

```groovy test-set=1 order=first,last
first = source.firstBy("Name")
last = source.lastBy("Name")
```

The [`lastBy`](../reference/table-operations/group-and-aggregate/lastBy.md) operation creates a new table containing the last row in the input table for each key.

The "last" row is defined as the row closest to the bottom of the table. It is based strictly on the order of the rows in the source table — not an aspect of the data (such as timestamps or sequence numbers) nor, for live-updating tables, the row which appeared in the dataset most recently.

A related operation is [`AggSortedLast`](../reference/table-operations/group-and-aggregate/AggSortedLast.md),
which sorts the data within each key before finding the last row. When used with `aggBy`, `AggSortedLast` takes
the column name (or an array of column names) to sort the data by before performing the `lastBy` operation to identify
the last row. It is more efficient than `.sort(<sort columns>).lastBy(<key columns>)` because the data is first
separated into groups (based on the key columns), then sorted within each group. In many cases this requires less memory
and processing than sorting the entire table at once.

The most basic case is a [`lastBy`](../reference/table-operations/group-and-aggregate/lastBy.md) with no key columns. When no key columns are specified, [`lastBy`](../reference/table-operations/group-and-aggregate/lastBy.md) simply returns the last row in the table:

```groovy order=t2,t
t = emptyTable(10).update("MyCol = ii + 1")
t2 = t.lastBy()
```

When key columns are specified (such as `MyKey` in the example below), [`lastBy`](../reference/table-operations/group-and-aggregate/lastBy.md) returns the last row for each key:

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

Often, [`lastBy`](../reference/table-operations/group-and-aggregate/lastBy.md) is used with time series data to return a table showing the current state of a time series. The example below demonstrates using [`lastBy`](../reference/table-operations/group-and-aggregate/lastBy.md) on time series data (using generated data, including timestamps generated based
on the current time).

```groovy order=t2,t
startTime = now()

t = emptyTable(100).update(
        "Timestamp = startTime + SECOND * ii",
        "MyKey = Long.toString(10 + ii % 5, 36).toUpperCase()",
        "MyCol = ii",
)

t2 = t.lastBy("MyKey")
```

When data is out of order, it can be sorted before applying the [`lastBy`](../reference/table-operations/group-and-aggregate/lastBy.md). For example, the data below is naturally
ordered by `Timestamp1`. In order to find the latest rows based on `Timestamp2`, simply sort the data before
running the [`lastBy`](../reference/table-operations/group-and-aggregate/lastBy.md):

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

If the sorted data is not used elsewhere in the query, a more efficient implementation is to use
[AggSortedLast](../reference/table-operations/group-and-aggregate/AggSortedLast.md). This produces the same
result table (`t2`) with one method call (and more efficeint processing of the `sort` step):

```groovy order=t2,t
startTime = now()

t = emptyTable(100).update(
        "Timestamp1 = startTime + SECOND * ii",
        "Timestamp2 = startTime + SECOND * randomInt(0, 100)",
        "MyKey = Long.toString(10 + ii % 5, 36).toUpperCase()",
        "MyCol = ii",
)

// last row by MyKey, after sorting by Timestamp2
t2 = t.aggBy(AggSortedLast("Timestamp2", "Timestamp1", "MyCol"), "MyKey")
```

### `headBy` and `tailBy`

In this example, we want to know the first two and the last two test results for each student. To achieve this, we can use [`headBy`](../reference/table-operations/group-and-aggregate/headBy.md) to return the first `n` test values and [`tailBy`](../reference/table-operations/group-and-aggregate/tailBy.md) to return the last `n` test value. The results are grouped by `Name`.

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

In the following examples, we start with the same source table containing students' test results as used above.

> [!CAUTION]
> Applying these aggregations to a column where the average cannot be computed will result in an error. For example, the average is not defined for a column of string values. For more information on removing columns from a table, see [`dropColumns`](../reference/table-operations/select/drop-columns.md). The syntax for using [`dropColumns`](../reference/table-operations/select/drop-columns.md) is `result = source.dropColumns("Col1", "Col2").sumBy("Col3", "Col4")`.

### `sumBy`

In this example, [`sumBy`](../reference/table-operations/group-and-aggregate/sumBy.md) calculates the total sum of test scores for each `Name`. Because a sum cannot be computed for the string column `Subject`, this column is dropped before applying [`sumBy`](../reference/table-operations/group-and-aggregate/sumBy.md).

```groovy test-set=1
sum = source.dropColumns("Subject").sumBy("Name")
```

### `avgBy`

In this example, [`avgBy`](../reference/table-operations/group-and-aggregate/avgBy.md) calculates the average (mean) of test scores for each `Name`. Because an average cannot be computed for the string column `Subject`, this column is dropped before applying [`avgBy`](../reference/table-operations/group-and-aggregate/avgBy.md).

```groovy test-set=1
mean = source.dropColumns("Subject").avgBy("Name")
```

### `stdBy`

In this example, [`stdBy`](../reference/table-operations/group-and-aggregate/stdBy.md) calculates the sample standard deviation of test scores for each `Name`. Because a sample standard deviation cannot be computed for the string column `Subject`, this column is dropped before applying [`stdBy`](../reference/table-operations/group-and-aggregate/stdBy.md).

```groovy test-set=1
stdDev = source.dropColumns("Subject").stdBy("Name")
```

### `varBy`

In this example, [`varBy`](../reference/table-operations/group-and-aggregate/varBy.md) calculates the sample variance of test scores for each `Name`. Because sample variance cannot be computed for the string column `Subject`, this column is dropped before applying [`varBy`](../reference/table-operations/group-and-aggregate/varBy.md).

```groovy test-set=1
var = source.dropColumns("Subject").varBy("Name")
```

### `medianBy`

In this example, [`medianBy`](../reference/table-operations/group-and-aggregate/medianBy.md) calculates the median of test scores for each `Name`. Because a median cannot be computed for the string column `Subject`, this column is dropped before applying [`medianBy`](../reference/table-operations/group-and-aggregate/medianBy.md).

```groovy test-set=1
median = source.dropColumns("Subject").medianBy("Name")
```

### `minBy`

In this example, [`minBy`](../reference/table-operations/group-and-aggregate/minBy.md) calculates the minimum of test scores for each `Name`. Because a minimum cannot be computed for the string column `Subject`, this column is dropped before applying [`minBy`](../reference/table-operations/group-and-aggregate/minBy.md).

```groovy test-set=1
minimum = source.dropColumns("Subject").minBy("Name")
```

### `maxBy`

In this example, [`maxBy`](../reference/table-operations/group-and-aggregate/maxBy.md) calculates the maximum of test scores for each `Name`. Because a maximum cannot be computed for the string column `Subject`, this column is dropped before applying [`maxBy`](../reference/table-operations/group-and-aggregate/maxBy.md) .

```groovy test-set=1
maximum = source.dropColumns("Subject").maxBy("Name")
```

## Aggregate columns in the UI

Single aggregations can be performed in the UI using the **Aggregate Columns** feature. However, rather than adding the aggregation in its own column as shown in the programmatic examples above, the aggregation is added as a row to the top or bottom of the table. Column data is aggregated using the operation of your choice. This is useful for quickly calculating the sum, average, or other aggregate of a column.

In the example below, we add an Average row and a Minimum row to the top of the table:

![A user aggregates columns in the UI with the **Aggregate Columns** feature](../assets/how-to/ui/aggregate_columns.gif)

These aggregations can be re-ordered, edited, or deleted from the **Aggregate Columns** dialog.

## Related documentation

- [How to create multiple summary statistics for groups](./combined-aggregations.md)
- [`avgBy`](../reference/table-operations/group-and-aggregate/avgBy.md)
- [`countBy`](../reference/table-operations/group-and-aggregate/countBy.md)
- [`dropColumns`](../reference/table-operations/select/drop-columns.md)
- [`firstBy`](../reference/table-operations/group-and-aggregate/firstBy.md)
- [`headBy`](../reference/table-operations/group-and-aggregate/headBy.md)
- [`lastBy`](../reference/table-operations/group-and-aggregate/lastBy.md)
- [`maxBy`](../reference/table-operations/group-and-aggregate/maxBy.md)
- [`medianBy`](../reference/table-operations/group-and-aggregate/medianBy.md)
- [`minBy`](../reference/table-operations/group-and-aggregate/minBy.md)
- [`stdBy`](../reference/table-operations/group-and-aggregate/stdBy.md)
- [`sumBy`](../reference/table-operations/group-and-aggregate/sumBy.md)
- [`tailBy`](../reference/table-operations/group-and-aggregate/tailBy.md)
- [`varBy`](../reference/table-operations/group-and-aggregate/varBy.md)
