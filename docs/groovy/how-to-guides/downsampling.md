---
title: Downsample data
---

Downsampling reduces the number of rows in a table by grouping rows by category or by time interval and [aggregating](../conceptual/aggregations.md) each group into a single row. It makes large or high-frequency data easier to plot, store, and analyze.

Downsampling works on live data, too. When rows are added to or change in the source table, the downsampled result updates with it. For details, see [Aggregations on ticking tables](../conceptual/aggregations.md#aggregations-on-ticking-tables).

This guide shows you how to downsample categorical data, how to downsample time data by putting timestamps into time bins, and where to go for more complex downsampling.

## Downsample categorical data

Let's say we have a dataset that tracks credit card purchases. Each entry in this dataset has a purchase amount and a purchase category.

This example downsamples the data with [`sumBy`](../reference/table-operations/group-and-aggregate/sumBy.md). The columns you group by are called key columns. Here, `PurchaseCategory` is the key column, so the result totals `PurchasePrice` for each category.

```groovy order=source,result
source = newTable(
    intCol("PurchasePrice", 30, 45, 35, 55, 25, 35),
    stringCol("PurchaseCategory", "Groceries", "Utilities", "Utilities", "Dining", "Groceries", "Dining")
)
result = source.sumBy("PurchaseCategory")
```

The result has one row for each of the three categories instead of six rows, which is the reduction downsampling aims for. Categorical data is easy to downsample because its values already fall into a small set of discrete categories.

## Downsample time data

Time data is harder to downsample than categorical data. Let's say we add a [date-time](../reference/query-language/types/date-time.md) stamp to each purchase. We want to sum the purchases by day and category. How can we downsample this data when nearly every timestamp is unique? Grouping on the [date-time](../reference/query-language/types/date-time.md) column directly doesn't shrink the table. Instead, put each timestamp into a time bin, and then group on the bin.

Deephaven has two built-in [query-language functions](../reference/query-language/query-library/auto-imported/time.md) that you use in formula strings to assign each [date-time](../reference/query-language/types/date-time.md) value to a time bin. A bin can be any duration, such as a second, a minute, an hour, or a day.

- [`lowerBin`](../reference/time/datetime/lowerBin.md) returns the start of the bin that contains a [date-time](../reference/query-language/types/date-time.md).
- [`upperBin`](../reference/time/datetime/upperBin.md) returns the end of the bin.

When a timestamp falls exactly on a bin boundary, both functions return it unchanged. So [`lowerBin`](../reference/time/datetime/lowerBin.md) puts it in the bin that starts at that boundary, and [`upperBin`](../reference/time/datetime/upperBin.md) puts it in the bin that ends there.

### Sum values in each time bin

This example builds on the credit card purchases table from [Downsample categorical data](#downsample-categorical-data) and sums the purchases by day and category:

1. Add a `TimeStamps` column of [date-time](../reference/query-language/types/date-time.md) values.
2. Add a `TimeStampsBinned` column that bins each timestamp by the day with [`lowerBin`](../reference/time/datetime/lowerBin.md).
3. [Drop](../reference/table-operations/select/drop-columns.md) the original `TimeStamps` column. [`sumBy`](../reference/table-operations/group-and-aggregate/sumBy.md) sums every column that isn't a key column, and it can't sum [date-time](../reference/query-language/types/date-time.md) values.
4. Use [`sumBy`](../reference/table-operations/group-and-aggregate/sumBy.md) with two key columns, `PurchaseCategory` and `TimeStampsBinned`.

[`lowerBin`](../reference/time/datetime/lowerBin.md) takes a bin size and an optional offset, both in nanoseconds or both as [`Duration`](../reference/query-language/types/durations.md) values. The built-in [`HOUR`](../reference/query-language/query-library/auto-imported/time.md) constant is one hour in nanoseconds, so `24 * HOUR` is one day.

Bin boundaries are counted from the Unix epoch, which is 1970-01-01T00:00:00 UTC. Without an offset, a one-day bin starts at midnight UTC. This example adds a five-hour offset, so each bin starts at midnight US Eastern Standard Time (UTC-5). The offset is fixed, so during daylight saving time the bins start at 1:00 AM Eastern. To align bins to a different time of day, change the offset. For details, see the `offset` parameter of [`lowerBin`](../reference/time/datetime/lowerBin.md).

```groovy order=source,result
t1 = parseInstant("2021-01-01T01:00:00 ET")
t2 = parseInstant("2021-01-01T05:00:00 ET")
t3 = parseInstant("2021-01-01T10:00:00 ET")
t4 = parseInstant("2021-01-02T01:00:00 ET")
t5 = parseInstant("2021-01-02T05:00:00 ET")
t6 = parseInstant("2021-01-02T10:00:00 ET")

source = newTable(
    intCol("PurchasePrice", 30, 45, 35, 55, 25, 35),
    stringCol("PurchaseCategory", "Groceries", "Utilities", "Utilities", "Dining", "Groceries", "Dining"),
    instantCol("TimeStamps", t1, t2, t3, t4, t5, t6)
)

result = source.update("TimeStampsBinned = lowerBin(TimeStamps, 24 * HOUR, 5 * HOUR)")
    .dropColumns("TimeStamps")
    .sumBy("PurchaseCategory", "TimeStampsBinned")
```

Each row of `result` is one category on one day, and each `TimeStampsBinned` value is midnight Eastern time. The six purchases become four rows. For example, the two `Utilities` purchases on January 1 sum to 80.

### Keep the last row in each time bin

A common way to downsample high-frequency time-series data is to bin the timestamps and keep the last row in each bin. This example makes one-minute bins with [`upperBin`](../reference/time/datetime/upperBin.md), so each bin is labeled with the time it ends. [`lowerBin`](../reference/time/datetime/lowerBin.md) works too. Then [`lastBy`](../reference/table-operations/group-and-aggregate/lastBy.md) keeps the last row in each bin.

The example adds the bin column with [`updateView`](../reference/table-operations/select/update-view.md), which computes the column on demand. [`update`](../reference/table-operations/select/update.md) works too, but it stores the column in memory.

```groovy order=tDownsampled,tBinned,t
startTime = parseInstant("2021-01-04T09:30:05 ET")

t = emptyTable(18).update("Timestamp = startTime + 10 * SECOND * ii", "Price = 100.0 + ii")

tBinned = t.updateView("TimeBin = upperBin(Timestamp, 1 * MINUTE)")

tDownsampled = tBinned.lastBy("TimeBin")
```

`t` has one row every 10 seconds for three minutes. [`ii`](../reference/query-language/variables/special-variables.md) is the row index, so each row is `10 * ii` seconds after `startTime`. Because [`upperBin`](../reference/time/datetime/upperBin.md) labels each row with the end of its bin, the rows from 09:30:05 to 09:30:55 all get the `TimeBin` 09:31:00. `tDownsampled` keeps only the last row of each minute, so the 18 rows of `t` become three.

### Keep the last row for each symbol in each time bin

To downsample prices for several ticker symbols at once, add `Symbol` as a second key column, so [`lastBy`](../reference/table-operations/group-and-aggregate/lastBy.md) keeps the last row for each symbol in each minute. As in [Sum values in each time bin](#sum-values-in-each-time-bin), pass several key columns. This example groups by `Symbol` and `TimeBin`:

```groovy order=tDownsampledMulti,tBinned,t
startTime = parseInstant("2021-01-04T09:30:05 ET")

t = emptyTable(18).update(
    "Timestamp = startTime + 10 * SECOND * ii",
    "Symbol = (ii % 2 == 0) ? `AAPL` : `MSFT`",
    "Price = 100.0 + ii"
)

tBinned = t.updateView("TimeBin = upperBin(Timestamp, 1 * MINUTE)")

tDownsampledMulti = tBinned.lastBy("Symbol", "TimeBin")
```

`tDownsampledMulti` has one row for each symbol in each minute: six rows in all.

## More complex downsampling

For more complex downsampling, use a [multi-aggregation](./combined-aggregations.md) with [`aggBy`](../reference/table-operations/group-and-aggregate/aggBy.md), which applies several [aggregations](../conceptual/aggregations.md) in one operation. For example, it can compute the minimum, maximum, average, weighted average, first value, and last value of each bin.

## Related documentation

- [Aggregations in Deephaven](../conceptual/aggregations.md)
- [Time in Deephaven](../conceptual/time-in-deephaven.md)
- [Single aggregation](./dedicated-aggregations.md)
- [Multi-aggregation](./combined-aggregations.md)
- [Date-time](../reference/query-language/types/date-time.md)
- [Durations](../reference/query-language/types/durations.md)
- [Time functions](../reference/query-language/query-library/auto-imported/time.md)
- [`aggBy`](../reference/table-operations/group-and-aggregate/aggBy.md)
- [`dropColumns`](../reference/table-operations/select/drop-columns.md)
- [`emptyTable`](../reference/table-operations/create/emptyTable.md)
- [`instantCol`](../reference/table-operations/create/instantCol.md)
- [`intCol`](../reference/table-operations/create/intCol.md)
- [`lastBy`](../reference/table-operations/group-and-aggregate/lastBy.md)
- [`lowerBin`](../reference/time/datetime/lowerBin.md)
- [`newTable`](../reference/table-operations/create/newTable.md)
- [`parseInstant`](../reference/time/datetime/parseInstant.md)
- [`stringCol`](../reference/table-operations/create/stringCol.md)
- [`sumBy`](../reference/table-operations/group-and-aggregate/sumBy.md)
- [`update`](../reference/table-operations/select/update.md)
- [`updateView`](../reference/table-operations/select/update-view.md)
- [`upperBin`](../reference/time/datetime/upperBin.md)
- [`DateTimeUtils` Javadoc](/core/javadoc/io/deephaven/time/DateTimeUtils.html)
