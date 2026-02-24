---
title: Single aggregation
---

This guide will show you how to compute summary information on groups of data using dedicated data aggregations.

Often when working with data, you will want to break the data into subgroups and then perform calculations on the grouped data. For example, a large multi-national corporation may want to know their average employee salary by country, or a teacher might want to calculate grade information for groups of students or in certain subject areas.

The process of breaking a table into subgroups and then performing a single type of calculation on the subgroups is known as "dedicated aggregation." The term comes from most operations creating a summary of data within a group (aggregation) and from a single type of operation being computed at once (dedicated).

Deephaven provides many dedicated aggregations, such as [`max_by`](../reference/table-operations/group-and-aggregate/maxBy.md) and [`min_by`](../reference/table-operations/group-and-aggregate/minBy.md). These aggregations are good options if only one type of aggregation is needed. If more than one type of aggregation is needed or if you have a custom aggregation, [combined aggregations](./combined-aggregations.md) are a more efficient and more flexible solution.

## Syntax

The general syntax follows:

```python skip-test
result = source.DEDICATED_AGG(by=["GroupingColumns"])
```

The `by = ["GroupingColumns"]` parameter determines the column(s) by which to group data.

- `DEDICATED_AGG` should be substituted with one of the chosen aggregations below.
- `[]` uses the whole table as a single group.
- `["X"]` will output the desired value for each group in column `X`.
- `["X", "Y"]` will output the desired value for each group designated from the `X` and `Y` columns.

## Single aggregators

Each dedicated aggregator performs one calculation at a time:

- [`abs_sum_by`](../reference/table-operations/group-and-aggregate/AbsSumBy.md) - Sum of absolute values of each group.
- [`avg_by`](../reference/table-operations/group-and-aggregate/avgBy.md) - Average (mean) of each group.
- [`count_by`](../reference/table-operations/group-and-aggregate/countBy.md) - Number of rows in each group.
- [`first_by`](../reference/table-operations/group-and-aggregate/firstBy.md) - First row of each group.
- [`group_by`](../reference/table-operations/group-and-aggregate/groupBy.md) - Group column content into vectors.
- [`head_by`](../reference/table-operations/group-and-aggregate/headBy.md) - First `n` rows of each group.
- [`last_by`](../reference/table-operations/group-and-aggregate/lastBy.md) - Last row of each group.
- [`max_by`](../reference/table-operations/group-and-aggregate/maxBy.md) - Maximum value of each group.
- [`median_by`](../reference/table-operations/group-and-aggregate/medianBy.md) - Median of each group.
- [`min_by`](../reference/table-operations/group-and-aggregate/minBy.md) - Minimum value of each group.
- [`std_by`](../reference/table-operations/group-and-aggregate/stdBy.md) - Sample standard deviation of each group.
- [`sum_by`](../reference/table-operations/group-and-aggregate/sumBy.md) - Sum of each group.
- [`tail_by`](../reference/table-operations/group-and-aggregate/tailBy.md) - Last `n` rows of each group.
- [`var_by`](../reference/table-operations/group-and-aggregate/varBy.md) - Sample variance of each group.
- [`weighted_avg_by`](../reference/table-operations/group-and-aggregate/weighted-sum-by.md) - Weighted average of each group.
- [`weighted_sum_by`](../reference/table-operations/group-and-aggregate/weighted-sum-by.md) - Weighted sum of each group.

In the following examples, we have test results in various subjects for some students. We want to summarize this information to see if students perform better in one class or another.

```python test-set=1
from deephaven import new_table
from deephaven.column import string_col, int_col, double_col

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

In this example, we want to know the first and the last test results for each student. To achieve this, we can use [`first_by`](../reference/table-operations/group-and-aggregate/firstBy.md) to return the first test value and [`last_by`](../reference/table-operations/group-and-aggregate/lastBy.md) to return the last test value. The results are grouped by `Name`.

```python test-set=1 order=first,last
first = source.first_by(by=["Name"])
last = source.last_by(by=["Name"])
```

The [`last_by`](../reference/table-operations/group-and-aggregate/lastBy.md) operation creates a new table containing the last row in the input table for each key.

The "last" row is defined as the row closest to the bottom of the table. It is based strictly on the order of the rows in the source table — not an aspect of the data (such as timestamps or sequence numbers) nor, for live-updating tables, the row which appeared in the dataset most recently.

A related operation is [`agg.sorted_last`](../reference/table-operations/group-and-aggregate/AggSortedLast.md), which sorts the data within each key before finding the last row. When used with `agg_by`, `agg.sorted_last` takes the column name (or an array of column names) to sort the data by before performing the `last_by` operation to identify the last row. It is more efficient than `.sort([<sort columns>]).last_by([<key columns>])` because the data is first separated into groups (based on the key columns), then sorted within each group. In many cases this requires less memory and processing than sorting the entire table at once.

The most basic case is a [`last_by`](../reference/table-operations/group-and-aggregate/lastBy.md) with no key columns. When no key columns are specified, [`last_by`](../reference/table-operations/group-and-aggregate/lastBy.md) simply returns the last row in the table:

```python order=t2,t
from deephaven import empty_table

t = empty_table(10).update("MyCol = ii + 1")
t2 = t.last_by()
```

When key columns are specified (such as `MyKey` in the example below), [`last_by`](../reference/table-operations/group-and-aggregate/lastBy.md) returns the last row for each key:

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

Often, [`last_by`](../reference/table-operations/group-and-aggregate/lastBy.md) is used with time series data to return a table showing the current state of a time series. The example below demonstrates using [`last_by`](../reference/table-operations/group-and-aggregate/lastBy.md) on time series data (using generated data, including timestamps generated based
on the current time).

```python order=t2,t
from deephaven import empty_table
from deephaven.time import dh_now

startTime = dh_now()
t = empty_table(100).update(
    [
        "Timestamp = startTime + SECOND * ii",
        "MyKey = Long.toString(10 + ii % 5, 36).toUpperCase()",
        "MyCol = ii",
    ]
)
t2 = t.last_by("MyKey")
```

When data is out of order, it can be sorted before applying the [`last_by`](../reference/table-operations/group-and-aggregate/lastBy.md). For example, the data below is naturally ordered by `Timestamp1`. In order to find the latest rows based on `Timestamp2`, simply sort the data before running the [`last_by`](../reference/table-operations/group-and-aggregate/lastBy.md):

```python test-set=2 order=t2,t_sorted,t
from deephaven import empty_table
from deephaven.time import dh_now

startTime = dh_now()
t = empty_table(100).update(
    [
        "Timestamp1 = startTime + SECOND * ii",
        "Timestamp2 = startTime + SECOND * randomInt(0, 100)",
        "MyKey = Long.toString(10 + ii % 5, 36).toUpperCase()",
        "MyCol = ii",
    ]
)
t_sorted = t.sort("Timestamp2")
t2 = t_sorted.last_by("MyKey")
```

If the sorted data is not used elsewhere in the query, a more efficient implementation is to use [agg.sorted_last](../reference/table-operations/group-and-aggregate/AggSortedLast.md). This produces the same result table (`t2`) with one method call (and more efficeint processing of the `sort` step):

```python order=t2,t
from deephaven import empty_table
from deephaven.time import dh_now
from deephaven import agg

startTime = dh_now()
t = empty_table(100).update(
    [
        "Timestamp1 = startTime + SECOND * ii",
        "Timestamp2 = startTime + SECOND * randomInt(0, 100)",
        "MyKey = Long.toString(10 + ii % 5, 36).toUpperCase()",
        "MyCol = ii",
    ]
)
t2 = t.agg_by(
    agg.sorted_last(order_by=["Timestamp2"], cols=["Timestamp1", "MyCol"]), by="MyKey"
)  # last row by MyKey, after sorting by Timestamp2
```

### `head_by` and `tail_by`

In this example, we want to know the first two and the last two test results for each student. To achieve this, we can use [`head_by`](../reference/table-operations/group-and-aggregate/headBy.md) to return the first `n` test values and [`tail_by`](../reference/table-operations/group-and-aggregate/tailBy.md) to return the last `n` test values. The results are grouped by `Name`.

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

In the following examples, we start with the same source table containing students' test results as used above.

> [!CAUTION]
> Applying these aggregations to a column where the average cannot be computed will result in an error. For example, the average is not defined for a column of string values. For more information on removing columns from a table, see [`drop_columns`](../reference/table-operations/select/drop-columns.md). The syntax for using [`drop_columns`](../reference/table-operations/select/drop-columns.md) is `result = source.drop_columns(cols=["Col1", "Col2"]).sum_by(by=["Col3", "Col4"])`.

### `sum_by`

In this example, [`sum_by`](../reference/table-operations/group-and-aggregate/sumBy.md) calculates the total sum of test scores for each `Name`. Because a sum cannot be computed for the string column `Subject`, this column is dropped before applying [`sum_by`](../reference/table-operations/group-and-aggregate/sumBy.md).

```python test-set=1
sum_table = source.drop_columns(cols=["Subject"]).sum_by(by=["Name"])
```

### `avg_by`

In this example, [`avg_by`](../reference/table-operations/group-and-aggregate/avgBy.md) calculates the average (mean) of test scores for each `Name`. Because an average cannot be computed for the string column `Subject`, this column is dropped before applying [`avg_by`](../reference/table-operations/group-and-aggregate/avgBy.md).

```python test-set=1
mean = source.drop_columns(cols=["Subject"]).avg_by(by=["Name"])
```

### `std_by`

In this example, [`std_by`](../reference/table-operations/group-and-aggregate/stdBy.md) calculates the sample standard deviation of test scores for each `Name`. Because a sample standard deviation cannot be computed for the string column `Subject`, this column is dropped before applying [`std_by`](../reference/table-operations/group-and-aggregate/stdBy.md).

```python test-set=1
std_dev = source.drop_columns(cols=["Subject"]).std_by(by=["Name"])
```

### `var_by`

In this example, [`var_by`](../reference/table-operations/group-and-aggregate/varBy.md) calculates the sample variance of test scores for each `Name`. Because sample variance cannot be computed for the string column `Subject`, this column is dropped before applying [`var_by`](../reference/table-operations/group-and-aggregate/varBy.md).

```python test-set=1
var = source.drop_columns(cols=["Subject"]).var_by(by=["Name"])
```

### `median_by`

In this example, [`median_by`](../reference/table-operations/group-and-aggregate/medianBy.md) calculates the median of test scores for each `Name`. Because a median cannot be computed for the string column `Subject`, this column is dropped before applying [`median_by`](../reference/table-operations/group-and-aggregate/medianBy.md).

```python test-set=1
median = source.drop_columns(cols=["Subject"]).median_by(by=["Name"])
```

### `min_by`

In this example, [`min_by`](../reference/table-operations/group-and-aggregate/minBy.md) calculates the minimum of test scores for each `Name`. Because a minimum cannot be computed for the string column `Subject`, this column is dropped before applying [`min_by`](../reference/table-operations/group-and-aggregate/minBy.md).

```python test-set=1
minimum = source.drop_columns(cols=["Subject"]).min_by(by=["Name"])
```

### `max_by`

In this example, [`max_by`](../reference/table-operations/group-and-aggregate/maxBy.md) calculates the maximum of test scores for each `Name`. Because a maximum cannot be computed for the string column `Subject`, this column is dropped before applying [`max_by`](../reference/table-operations/group-and-aggregate/maxBy.md) .

```python test-set=1
maximum = source.drop_columns(cols=["Subject"]).max_by(by=["Name"])
```

## Aggregate columns in the UI

Single aggregations can be performed in the UI using the **Aggregate Columns** feature. However, rather than adding the aggregation in its own column as shown in the programmatic examples above, the aggregation is added as a row to the top or bottom of the table. Column data is aggregated using the operation of your choice. This is useful for quickly calculating the sum, average, or other aggregate of a column.

In the example below, we add an Average row and a Minimum row to the top of the table:

![A user aggregates columns in the UI with the **Aggregate Columns** feature](../assets/how-to/ui/aggregate_columns.gif)

These aggregations can be re-ordered, edited, or deleted from the **Aggregate Columns** dialog.

## Related documentation

- [How to create multiple summary statistics for groups](./combined-aggregations.md)
- [`avg_by`](../reference/table-operations/group-and-aggregate/avgBy.md)
- [`count_by`](../reference/table-operations/group-and-aggregate/countBy.md)
- [`drop_columns`](../reference/table-operations/select/drop-columns.md)
- [`first_by`](../reference/table-operations/group-and-aggregate/firstBy.md)
- [`head_by`](../reference/table-operations/group-and-aggregate/headBy.md)
- [`last_by`](../reference/table-operations/group-and-aggregate/lastBy.md)
- [`max_by`](../reference/table-operations/group-and-aggregate/maxBy.md)
- [`median_by`](../reference/table-operations/group-and-aggregate/medianBy.md)
- [`min_by`](../reference/table-operations/group-and-aggregate/minBy.md)
- [`std_by`](../reference/table-operations/group-and-aggregate/stdBy.md)
- [`sum_by`](../reference/table-operations/group-and-aggregate/sumBy.md)
- [`tail_by`](../reference/table-operations/group-and-aggregate/tailBy.md)
- [`var_by`](../reference/table-operations/group-and-aggregate/varBy.md)
