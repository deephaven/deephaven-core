---
title: Single aggregation
---

This guide shows you how to compute summary information on groups of data using single (dedicated) aggregations.

Often when working with data, you want to break the data into subgroups and then perform calculations on the grouped data. For example, a multinational corporation might want to know its average employee salary by country, or a teacher might want to calculate grade information for groups of students or in certain subject areas.

The process of breaking a table into subgroups and then performing a single type of calculation on each subgroup is known as "single aggregation" or "dedicated aggregation." "Aggregation" refers to creating a summary of the data within each group. "Single" or "dedicated" means that only one type of calculation runs at a time.

To compare single aggregations with the other ways Deephaven summarizes data, and to choose between them, see [Aggregations in Deephaven](../conceptual/aggregations.md).

Deephaven provides many dedicated aggregations, such as [`max_by`](../reference/table-operations/group-and-aggregate/maxBy.md) and [`min_by`](../reference/table-operations/group-and-aggregate/minBy.md). These aggregations are good options when you need only one type of aggregation. If you need more than one type of aggregation or have a custom aggregation, [multi-aggregation](./combined-aggregations.md), also called combined aggregation, is a more efficient and more flexible solution.

## Syntax

The general syntax is:

```python skip-test
result = source.DEDICATED_AGG(by=["GroupingColumns"])
```

Replace `DEDICATED_AGG` with one of the aggregators listed in [Single aggregators](#single-aggregators). The `by` parameter determines the column(s) by which to group data:

- Omitting `by` (for example, `source.last_by()`) or passing `[]` uses the whole table as a single group.
- `["X"]` outputs the desired value for each group in column `X`.
- `["X", "Y"]` outputs the desired value for each group designated from the `X` and `Y` columns.
- `"X"`, a single column name passed as a string, is the same as `["X"]`.

You can also pass `by` by position instead of by keyword, so `source.last_by(["Name"])` is the same as `source.last_by(by=["Name"])`.

Some aggregators take a required argument before the grouping columns:

- [`head_by`](../reference/table-operations/group-and-aggregate/headBy.md) and [`tail_by`](../reference/table-operations/group-and-aggregate/tailBy.md) take the number of rows.
- [`count_by`](../reference/table-operations/group-and-aggregate/countBy.md) takes the name of the count column.
- [`weighted_avg_by`](../reference/table-operations/group-and-aggregate/weighted-avg-by.md) and [`weighted_sum_by`](../reference/table-operations/group-and-aggregate/weighted-sum-by.md) take the weight column.

## Single aggregators

Each dedicated aggregator performs one calculation at a time:

- [`abs_sum_by`](../reference/table-operations/group-and-aggregate/AbsSumBy.md) - Sum of absolute values of each group.
- [`avg_by`](../reference/table-operations/group-and-aggregate/avgBy.md) - Average (mean) of each group.
- [`count_by`](../reference/table-operations/group-and-aggregate/countBy.md) - Number of rows in each group.
- [`first_by`](../reference/table-operations/group-and-aggregate/firstBy.md) - First row of each group.
- [`group_by`](../reference/table-operations/group-and-aggregate/groupBy.md) - Group column content into [vectors](../conceptual/arrays-vs-vectors.md), one row per group.
- [`head_by`](../reference/table-operations/group-and-aggregate/headBy.md) - First `n` rows of each group.
- [`last_by`](../reference/table-operations/group-and-aggregate/lastBy.md) - Last row of each group.
- [`max_by`](../reference/table-operations/group-and-aggregate/maxBy.md) - Maximum value of each group.
- [`median_by`](../reference/table-operations/group-and-aggregate/medianBy.md) - Median of each group.
- [`min_by`](../reference/table-operations/group-and-aggregate/minBy.md) - Minimum value of each group.
- [`std_by`](../reference/table-operations/group-and-aggregate/stdBy.md) - Sample standard deviation of each group.
- [`sum_by`](../reference/table-operations/group-and-aggregate/sumBy.md) - Sum of each group.
- [`tail_by`](../reference/table-operations/group-and-aggregate/tailBy.md) - Last `n` rows of each group.
- [`var_by`](../reference/table-operations/group-and-aggregate/varBy.md) - Sample variance of each group.
- [`weighted_avg_by`](../reference/table-operations/group-and-aggregate/weighted-avg-by.md) - Weighted average of each group.
- [`weighted_sum_by`](../reference/table-operations/group-and-aggregate/weighted-sum-by.md) - Weighted sum of each group.

The examples on this page cover every aggregator in this list except [`abs_sum_by`](../reference/table-operations/group-and-aggregate/AbsSumBy.md), [`group_by`](../reference/table-operations/group-and-aggregate/groupBy.md), [`weighted_avg_by`](../reference/table-operations/group-and-aggregate/weighted-avg-by.md), and [`weighted_sum_by`](../reference/table-operations/group-and-aggregate/weighted-sum-by.md). For those four, see the examples on their reference pages.

## Select and count rows in each group

In the following examples, we have test results in various subjects for some students. We want to summarize each student's results.

```python test-set=1
from deephaven import new_table
from deephaven.column import string_col, int_col

source = new_table(
    [
        string_col(
            "Name",
            [
                "James",
                "James",
                "James",
                "Lauren",
                "Lauren",
                "Lauren",
                "Zoey",
                "Zoey",
                "Zoey",
            ],
        ),
        string_col(
            "Subject",
            [
                "Math",
                "Science",
                "Art",
                "Math",
                "Science",
                "Art",
                "Math",
                "Science",
                "Art",
            ],
        ),
        int_col("Number", [95, 100, 90, 72, 78, 92, 100, 98, 96]),
    ]
)
```

### `first_by` and `last_by`

In this example, we want to know the first and the last test results for each student. To achieve this, we can use [`first_by`](../reference/table-operations/group-and-aggregate/firstBy.md) to return the first test value and [`last_by`](../reference/table-operations/group-and-aggregate/lastBy.md) to return the last test value. The example groups the results by `Name`.

```python test-set=1 order=first,last
first = source.first_by(by=["Name"])
last = source.last_by(by=["Name"])
```

For how [`last_by`](../reference/table-operations/group-and-aggregate/lastBy.md) decides which row is last, and how to get the last row after sorting by a column, see [Find the last row for each key](#find-the-last-row-for-each-key-with-last_by).

### `head_by` and `tail_by`

In this example, we want to know the first two and the last two test results for each student. To achieve this, we can use [`head_by`](../reference/table-operations/group-and-aggregate/headBy.md) to return the first two test values and [`tail_by`](../reference/table-operations/group-and-aggregate/tailBy.md) to return the last two test values. The example groups the results by `Name`.

```python test-set=1 order=head,tail
head = source.head_by(2, by=["Name"])
tail = source.tail_by(2, by=["Name"])
```

### `count_by`

In this example, we want to know the number of tests each student completed. [`count_by`](../reference/table-operations/group-and-aggregate/countBy.md) returns the number of rows in the table as grouped by `Name` and stores that in a new column, `NumTests`.

```python test-set=1
count = source.count_by("NumTests", by=["Name"])
```

## Summary statistics aggregators

The following examples use the `source` table of student test results created in [Select and count rows in each group](#select-and-count-rows-in-each-group). It has a `Name` column, a string `Subject` column, and an integer `Number` column of scores. Because of the type restrictions below, the examples remove the string column `Subject` with [`drop_columns`](../reference/table-operations/select/drop-columns.md), so each result contains only score statistics.

> [!CAUTION]
> [`sum_by`](../reference/table-operations/group-and-aggregate/sumBy.md), [`avg_by`](../reference/table-operations/group-and-aggregate/avgBy.md), [`std_by`](../reference/table-operations/group-and-aggregate/stdBy.md), and [`var_by`](../reference/table-operations/group-and-aggregate/varBy.md) raise an error on a column of an unsupported type, such as a string column. These aggregators accept string columns:
>
> - [`median_by`](../reference/table-operations/group-and-aggregate/medianBy.md) returns the middle value in sort order.
> - [`min_by`](../reference/table-operations/group-and-aggregate/minBy.md) returns the smallest value in sort order.
> - [`max_by`](../reference/table-operations/group-and-aggregate/maxBy.md) returns the largest value in sort order.
>
> In sort order, uppercase letters come before lowercase letters.

### `sum_by`

In this example, [`sum_by`](../reference/table-operations/group-and-aggregate/sumBy.md) calculates the sum of test scores for each `Name`.

```python test-set=1
sum_table = source.drop_columns(cols=["Subject"]).sum_by(by=["Name"])
```

### `avg_by`

In this example, [`avg_by`](../reference/table-operations/group-and-aggregate/avgBy.md) calculates the average (mean) of test scores for each `Name`.

```python test-set=1
mean = source.drop_columns(cols=["Subject"]).avg_by(by=["Name"])
```

### `std_by`

In this example, [`std_by`](../reference/table-operations/group-and-aggregate/stdBy.md) calculates the sample standard deviation of test scores for each `Name`.

```python test-set=1
std_dev = source.drop_columns(cols=["Subject"]).std_by(by=["Name"])
```

### `var_by`

In this example, [`var_by`](../reference/table-operations/group-and-aggregate/varBy.md) calculates the sample variance of test scores for each `Name`.

```python test-set=1
var = source.drop_columns(cols=["Subject"]).var_by(by=["Name"])
```

### `median_by`

In this example, [`median_by`](../reference/table-operations/group-and-aggregate/medianBy.md) calculates the median of test scores for each `Name`.

```python test-set=1
median = source.drop_columns(cols=["Subject"]).median_by(by=["Name"])
```

### `min_by`

In this example, [`min_by`](../reference/table-operations/group-and-aggregate/minBy.md) calculates the minimum of test scores for each `Name`.

```python test-set=1
minimum = source.drop_columns(cols=["Subject"]).min_by(by=["Name"])
```

### `max_by`

In this example, [`max_by`](../reference/table-operations/group-and-aggregate/maxBy.md) calculates the maximum of test scores for each `Name`.

```python test-set=1
maximum = source.drop_columns(cols=["Subject"]).max_by(by=["Name"])
```

## Find the last row for each key with `last_by`

The [`last_by`](../reference/table-operations/group-and-aggregate/lastBy.md) operation creates a new table that contains the last row of the input table for each key. The key columns are the grouping columns you pass to `last_by`. A key is one unique value or combination of values in those columns. [`first_by`](../reference/table-operations/group-and-aggregate/firstBy.md) follows the same rules but returns the first row for each key.

The "last" row is the row closest to the bottom of the table. It depends only on the order of rows in the source table. It does not depend on values in the data, such as timestamps or sequence numbers. In a live-updating table, it is not necessarily the row that most recently arrived.

When you pass no key columns, [`last_by`](../reference/table-operations/group-and-aggregate/lastBy.md) returns the last row in the table:

```python order=t2,t
from deephaven import empty_table

t = empty_table(10).update("MyCol = ii + 1")
t2 = t.last_by()
```

When you pass key columns (such as `MyKey` in the example below), [`last_by`](../reference/table-operations/group-and-aggregate/lastBy.md) returns the last row for each key:

```python order=t2,t
from deephaven import empty_table

t = empty_table(10).update(
    ["MyKey = Long.toString(10 + ii % 5, 36).toUpperCase()", "MyCol = ii"]
)
t2 = t.last_by("MyKey")
```

You can use multiple key columns:

```python order=t2,t
from deephaven import empty_table

t = empty_table(10).update(
    [
        "MyKey = Long.toString(10 + ii % 5, 36).toUpperCase()",
        "MySecondKey = ii % 2",
        "MyCol = ii",
    ]
)
t2 = t.last_by(["MyKey", "MySecondKey"])
```

You often use [`last_by`](../reference/table-operations/group-and-aggregate/lastBy.md) with time series data to get a table that shows the current state of each key. When data is out of order, sort it before applying `last_by`. For example, the data below is naturally ordered by `Timestamp1`. To find the latest rows based on `Timestamp2`, [`sort`](../reference/table-operations/sort/sort.md) the data before running `last_by`:

```python test-set=2 order=t2,t_sorted,t
from deephaven import empty_table
from deephaven.time import dh_now

start_time = dh_now()
t = empty_table(100).update(
    [
        "Timestamp1 = start_time + SECOND * ii",
        "Timestamp2 = start_time + SECOND * randomInt(0, 100)",
        "MyKey = Long.toString(10 + ii % 5, 36).toUpperCase()",
        "MyCol = ii",
    ]
)
t_sorted = t.sort("Timestamp2")
t2 = t_sorted.last_by("MyKey")
```

If the query doesn't use the sorted table anywhere else, use [`agg.sorted_last`](../reference/table-operations/group-and-aggregate/AggSortedLast.md) instead of sorting first. It avoids sorting the whole table:

- It splits the rows into groups by the key columns.
- Within each group, it finds the row with the largest sort-column value.
- It doesn't build a sorted copy of the whole table, so it typically uses less memory and processing than [`sort`](../reference/table-operations/sort/sort.md) followed by [`last_by`](../reference/table-operations/group-and-aggregate/lastBy.md).

To use it, pass [`agg.sorted_last`](../reference/table-operations/group-and-aggregate/AggSortedLast.md) to [`agg_by`](../reference/table-operations/group-and-aggregate/aggBy.md) and set `order_by` to the column to sort by. The example below reuses `t` from the previous example and picks the same row for each key as that example's `t2`, without creating an intermediate sorted table. The key column comes first and rows can appear in a different order. Unlike [`last_by`](../reference/table-operations/group-and-aggregate/lastBy.md), it returns only the key columns and the columns listed in `cols`, so the example lists `Timestamp2` too.

```python test-set=2 order=t3
from deephaven import agg

t3 = t.agg_by(
    agg.sorted_last(order_by="Timestamp2", cols=["Timestamp1", "Timestamp2", "MyCol"]),
    by="MyKey",
)  # last row by MyKey, after sorting by Timestamp2
```

## Aggregate columns in the UI

You can also aggregate a column in the UI with the **Aggregate Columns** feature. Unlike the programmatic examples above, it doesn't create a new table with one row per group. Instead, it adds a row at the top or bottom of the table that applies the operation you choose to each column. Use it to quickly see the sum, average, or another aggregate of a column.

In the example below, we add an Average row and a Minimum row to the top of the table:

![A user aggregates columns in the UI with the **Aggregate Columns** feature](../assets/how-to/ui/aggregate_columns.gif)

You can reorder, edit, or delete these aggregations in the **Aggregate Columns** dialog.

## Related documentation

- [Aggregations in Deephaven](../conceptual/aggregations.md)
- [Multi-aggregation](./combined-aggregations.md)
- [`abs_sum_by`](../reference/table-operations/group-and-aggregate/AbsSumBy.md)
- [`agg.sorted_last`](../reference/table-operations/group-and-aggregate/AggSortedLast.md)
- [`agg_by`](../reference/table-operations/group-and-aggregate/aggBy.md)
- [`avg_by`](../reference/table-operations/group-and-aggregate/avgBy.md)
- [`count_by`](../reference/table-operations/group-and-aggregate/countBy.md)
- [`drop_columns`](../reference/table-operations/select/drop-columns.md)
- [`first_by`](../reference/table-operations/group-and-aggregate/firstBy.md)
- [`group_by`](../reference/table-operations/group-and-aggregate/groupBy.md)
- [`head_by`](../reference/table-operations/group-and-aggregate/headBy.md)
- [`last_by`](../reference/table-operations/group-and-aggregate/lastBy.md)
- [`max_by`](../reference/table-operations/group-and-aggregate/maxBy.md)
- [`median_by`](../reference/table-operations/group-and-aggregate/medianBy.md)
- [`min_by`](../reference/table-operations/group-and-aggregate/minBy.md)
- [`sort`](../reference/table-operations/sort/sort.md)
- [`std_by`](../reference/table-operations/group-and-aggregate/stdBy.md)
- [`sum_by`](../reference/table-operations/group-and-aggregate/sumBy.md)
- [`tail_by`](../reference/table-operations/group-and-aggregate/tailBy.md)
- [`var_by`](../reference/table-operations/group-and-aggregate/varBy.md)
- [`weighted_avg_by`](../reference/table-operations/group-and-aggregate/weighted-avg-by.md)
- [`weighted_sum_by`](../reference/table-operations/group-and-aggregate/weighted-sum-by.md)
