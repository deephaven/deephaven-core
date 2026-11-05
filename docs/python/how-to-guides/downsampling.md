---
title: Downsample data
---

Downsampling reduces the number of rows in a table by grouping rows by category or by time interval and [aggregating](../conceptual/aggregations.md) each group into a single row. It makes large or high-frequency data easier to plot, store, and analyze.

Downsampling works on live data, too. When rows are added to or change in the source table, the downsampled result updates with it. For details, see [Aggregations on ticking tables](../conceptual/aggregations.md#aggregations-on-ticking-tables).

This guide shows you how to downsample categorical data, how to downsample time data by putting timestamps into time bins, and where to go for more complex downsampling.

## Downsample categorical data

Let's say we have a dataset that tracks credit card purchases. Each entry in this dataset has a purchase amount and a purchase category.

This example downsamples the data with [`sum_by`](../reference/table-operations/group-and-aggregate/sumBy.md). The columns you group by are called key columns. Here, `PurchaseCategory` is the key column, so the result totals `PurchasePrice` for each category.

```python order=source,result
from deephaven import new_table
from deephaven.column import string_col, int_col

source = new_table(
    [
        int_col("PurchasePrice", [30, 45, 35, 55, 25, 35]),
        string_col(
            "PurchaseCategory",
            ["Groceries", "Utilities", "Utilities", "Dining", "Groceries", "Dining"],
        ),
    ]
)

result = source.sum_by(by=["PurchaseCategory"])
```

The result has one row for each of the three categories instead of six rows, which is the reduction downsampling aims for. Categorical data is easy to downsample because its values already fall into a small set of discrete categories.

## Downsample time data

Time data is harder to downsample than categorical data. Let's say we add a [date-time](../reference/query-language/types/date-time.md) stamp to each purchase. We want to sum the purchases by day and category. How can we downsample this data when nearly every timestamp is unique? Grouping on the [date-time](../reference/query-language/types/date-time.md) column directly doesn't shrink the table. Instead, put each timestamp into a time bin, and then group on the bin.

Deephaven has two built-in [query-language functions](../reference/query-language/query-library/auto-imported/time.md) that you use in formula strings to assign each [date-time](../reference/query-language/types/date-time.md) value to a time bin. A bin can be any duration, such as a second, a minute, an hour, or a day.

- [`lowerBin`](/core/javadoc/io/deephaven/time/DateTimeUtils.html#lowerBin(java.time.Instant,long,long)) returns the start of the bin that contains a [date-time](../reference/query-language/types/date-time.md).
- [`upperBin`](/core/javadoc/io/deephaven/time/DateTimeUtils.html#upperBin(java.time.Instant,long)) returns the end of the bin.

When a timestamp falls exactly on a bin boundary, both functions return it unchanged. So [`lowerBin`](/core/javadoc/io/deephaven/time/DateTimeUtils.html#lowerBin(java.time.Instant,long,long)) puts it in the bin that starts at that boundary, and [`upperBin`](/core/javadoc/io/deephaven/time/DateTimeUtils.html#upperBin(java.time.Instant,long)) puts it in the bin that ends there.

### Sum values in each time bin

This example builds on the credit card purchases table from [Downsample categorical data](#downsample-categorical-data) and sums the purchases by day and category:

1. Add a `TimeStamps` column of [date-time](../reference/query-language/types/date-time.md) values.
2. Add a `TimeStampsBinned` column that bins each timestamp by the day with [`lowerBin`](/core/javadoc/io/deephaven/time/DateTimeUtils.html#lowerBin(java.time.Instant,long,long)).
3. [Drop](../reference/table-operations/select/drop-columns.md) the original `TimeStamps` column. [`sum_by`](../reference/table-operations/group-and-aggregate/sumBy.md) sums every column that isn't a key column, and it can't sum [date-time](../reference/query-language/types/date-time.md) values.
4. Use [`sum_by`](../reference/table-operations/group-and-aggregate/sumBy.md) with two key columns, `PurchaseCategory` and `TimeStampsBinned`.

[`lowerBin`](/core/javadoc/io/deephaven/time/DateTimeUtils.html#lowerBin(java.time.Instant,long,long)) takes a bin size and an optional offset, both in nanoseconds or both as [`Duration`](../reference/query-language/types/durations.md) values. The built-in [`HOUR`](../reference/query-language/query-library/auto-imported/time.md) constant is one hour in nanoseconds, so `24 * HOUR` is one day.

Bin boundaries are counted from the Unix epoch, which is 1970-01-01T00:00:00 UTC. Without an offset, a one-day bin starts at midnight UTC. This example adds a five-hour offset, so each bin starts at midnight US Eastern Standard Time (UTC-5). The offset is fixed, so during daylight saving time the bins start at 1:00 AM Eastern. To align bins to a different time of day, see [How can I bin times to a specific time?](../reference/community-questions/bin-times-specific-time.md).

```python order=source,result
from deephaven import new_table
from deephaven.column import string_col, int_col, datetime_col
from deephaven.time import to_j_instant

t1 = to_j_instant("2021-01-01T01:00:00 ET")
t2 = to_j_instant("2021-01-01T05:00:00 ET")
t3 = to_j_instant("2021-01-01T10:00:00 ET")
t4 = to_j_instant("2021-01-02T01:00:00 ET")
t5 = to_j_instant("2021-01-02T05:00:00 ET")
t6 = to_j_instant("2021-01-02T10:00:00 ET")

source = new_table(
    [
        int_col("PurchasePrice", [30, 45, 35, 55, 25, 35]),
        string_col(
            "PurchaseCategory",
            ["Groceries", "Utilities", "Utilities", "Dining", "Groceries", "Dining"],
        ),
        datetime_col("TimeStamps", [t1, t2, t3, t4, t5, t6]),
    ]
)

result = (
    source.update(["TimeStampsBinned = lowerBin(TimeStamps, 24 * HOUR, 5 * HOUR)"])
    .drop_columns(cols=["TimeStamps"])
    .sum_by(by=["PurchaseCategory", "TimeStampsBinned"])
)
```

Each row of `result` is one category on one day, and each `TimeStampsBinned` value is midnight Eastern time. The six purchases become four rows. For example, the two `Utilities` purchases on January 1 sum to 80.

### Keep the last row in each time bin

A common way to downsample high-frequency time-series data is to bin the timestamps and keep the last row in each bin. This example makes one-minute bins with [`upperBin`](/core/javadoc/io/deephaven/time/DateTimeUtils.html#upperBin(java.time.Instant,long)), so each bin is labeled with the time it ends. [`lowerBin`](/core/javadoc/io/deephaven/time/DateTimeUtils.html#lowerBin(java.time.Instant,long,long)) works too. Then [`last_by`](../reference/table-operations/group-and-aggregate/lastBy.md) keeps the last row in each bin.

The example adds the bin column with [`update_view`](../reference/table-operations/select/update-view.md), which computes the column on demand. [`update`](../reference/table-operations/select/update.md) works too, but it stores the column in memory.

```python order=t_downsampled,t_binned,t
from deephaven import empty_table
from deephaven.time import to_j_instant

start_time = to_j_instant("2021-01-04T09:30:05 ET")
t = empty_table(18).update(
    ["Timestamp = start_time + 10 * SECOND * ii", "Price = 100.0 + ii"]
)
t_binned = t.update_view("TimeBin = upperBin(Timestamp, 1 * MINUTE)")
t_downsampled = t_binned.last_by("TimeBin")
```

`t` has one row every 10 seconds for three minutes. [`ii`](../reference/query-language/variables/special-variables.md) is the row index, so each row is `10 * ii` seconds after `start_time`. Because [`upperBin`](/core/javadoc/io/deephaven/time/DateTimeUtils.html#upperBin(java.time.Instant,long)) labels each row with the end of its bin, the rows from 09:30:05 to 09:30:55 all get the `TimeBin` 09:31:00. `t_downsampled` keeps only the last row of each minute, so the 18 rows of `t` become three.

### Keep the last row for each symbol in each time bin

To downsample prices for several ticker symbols at once, add `Symbol` as a second key column, so [`last_by`](../reference/table-operations/group-and-aggregate/lastBy.md) keeps the last row for each symbol in each minute. As in [Sum values in each time bin](#sum-values-in-each-time-bin), pass a list of key columns. This example groups by `Symbol` and `TimeBin`:

```python order=t_downsampled_multi,t_binned,t
from deephaven import empty_table
from deephaven.time import to_j_instant

start_time = to_j_instant("2021-01-04T09:30:05 ET")
t = empty_table(18).update(
    [
        "Timestamp = start_time + 10 * SECOND * ii",
        "Symbol = (ii % 2 == 0) ? `AAPL` : `MSFT`",
        "Price = 100.0 + ii",
    ]
)
t_binned = t.update_view("TimeBin = upperBin(Timestamp, 1 * MINUTE)")
t_downsampled_multi = t_binned.last_by(["Symbol", "TimeBin"])
```

`t_downsampled_multi` has one row for each symbol in each minute: six rows in all.

## More complex downsampling

For more complex downsampling, use a [multi-aggregation](./combined-aggregations.md) with [`agg_by`](../reference/table-operations/group-and-aggregate/aggBy.md), which applies several [aggregations](../conceptual/aggregations.md) in one operation. For example, it can compute the minimum, maximum, average, weighted average, first value, and last value of each bin.

## Related documentation

- [Aggregations in Deephaven](../conceptual/aggregations.md)
- [Time in Deephaven](../conceptual/time-in-deephaven.md)
- [Single aggregation](./dedicated-aggregations.md)
- [Multi-aggregation](./combined-aggregations.md)
- [How can I bin times to a specific time?](../reference/community-questions/bin-times-specific-time.md)
- [Date-time](../reference/query-language/types/date-time.md)
- [Durations](../reference/query-language/types/durations.md)
- [Time functions](../reference/query-language/query-library/auto-imported/time.md)
- [`agg_by`](../reference/table-operations/group-and-aggregate/aggBy.md)
- [`datetime_col`](../reference/table-operations/create/dateTimeCol.md)
- [`drop_columns`](../reference/table-operations/select/drop-columns.md)
- [`empty_table`](../reference/table-operations/create/emptyTable.md)
- [`int_col`](../reference/table-operations/create/intCol.md)
- [`last_by`](../reference/table-operations/group-and-aggregate/lastBy.md)
- [`new_table`](../reference/table-operations/create/newTable.md)
- [`string_col`](../reference/table-operations/create/stringCol.md)
- [`sum_by`](../reference/table-operations/group-and-aggregate/sumBy.md)
- [`to_j_instant`](../reference/time/datetime/to_j_instant.md)
- [`update`](../reference/table-operations/select/update.md)
- [`update_view`](../reference/table-operations/select/update-view.md)
- [`DateTimeUtils` Javadoc](/core/javadoc/io/deephaven/time/DateTimeUtils.html)
