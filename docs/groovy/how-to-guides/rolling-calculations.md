---
title: Cumulative, rolling, and moving statistics with update_by
sidebar_label: Cumulative, rolling, and moving statistics
---

This guide explains how to use Deephaven's [`updateBy`](../reference/table-operations/update-by-operations/updateBy.md) table operation and the [`updateby`](/core/javadoc/io/deephaven/api/updateby/package-summary.html) Groovy module to add cumulative, rolling, and moving statistics to a table.

## `updateBy` vs `updateby`

This document refers to [`updateBy`](../reference/table-operations/update-by-operations/updateBy.md) and [`updateby`](/core/javadoc/io/deephaven/api/updateby/package-summary.html) throughout. They are _not_ identical:

- [`updateBy`](../reference/table-operations/update-by-operations/updateBy.md) always refers to the[`updateBy`](../reference/table-operations/update-by-operations/updateBy.md) _table operation_. This is always invoked as a method on a table:

```groovy skip-test
result = source.updateBy(...)
```

- [`updateby`](/core/javadoc/io/deephaven/api/updateby/package-summary.html) refers to the Groovy package module housing all of the functions and related plumbing. The [`UpdateByOperation`](/core/javadoc/io/deephaven/api/updateby/UpdateByOperation.html) interface contains the `updateBy` operations themselves, such as `EmMax` and `RollingFormula`.

This distinction will be important to keep in mind as you progress through the document.

## Cumulative statistics

Cumulative statistics are the simplest [`UpdateByOperation`](/core/javadoc/io/deephaven/api/updateby/UpdateByOperation.html)s. They are ordinary statistics computed over all previous data. The following cumulative statistics are currently supported:

| Cumulative statistic | [`updateBy`](/core/javadoc/io/deephaven/api/updateby/package-summary.html) function |
| -------------------- | ----------------------------------------------------------------------------------- |
| Minimum              | [`CumMin`](../reference/table-operations/update-by-operations/cum-min.md)           |
| Maximum              | [`CumMax`](../reference/table-operations/update-by-operations/cum-max.md)           |
| Sum                  | [`CumSum`](../reference/table-operations/update-by-operations/cum-sum.md)           |
| Product              | [`CumProd`](../reference/table-operations/update-by-operations/cum-prod.md)         |

### Cumulative sum

To illustrate a cumulative statistic, consider the cumulative sum. This operation computes the sum of all previous data points for every row in a table. Here's an illustration of the cumulative sum:

![A diagram illustrating how a cumulative sum is calculated](../assets/how-to/rolling-calculations-3.png)

The fourth element of the cumulative sum column is the sum of the first four data points, the fifth element is the sum of the first five data points, and so on.

This calculation is implemented in Deephaven with the [`CumSum`](../reference/table-operations/update-by-operations/cum-sum.md) function and the [`updateBy`](../reference/table-operations/update-by-operations/updateBy.md) table operation:

```groovy order=result
source = emptyTable(8).update("X = ii")

result = source.updateBy(CumSum("SumX=X"))
```

Here, the `"SumX=X"` argument indicates that the resulting column will be renamed `SumX`.

### Cumulative average

The [`updateby`](/core/javadoc/io/deephaven/api/updateby/package-summary.html) module does not directly support a function to compute the cumulative average of a column. However, you can still compute the cumulative average by using two [`CumSum`](../reference/table-operations/update-by-operations/cum-sum.md) operations, where one of them is applied over a column of ones:

```groovy order=result
source = emptyTable(8).update("X = ii")

result = (
    source.update("Ones = 1")
    .updateBy(CumSum("SumX=X", "Ones"))
    .update("CumAvgX = SumX / Ones")
    .dropColumns("SumX", "Ones")
)
```

This demonstrates the flexibility of the [`update_by`](../reference/table-operations/update-by-operations/updateBy.md) table operation. If a particular kind of calculation is unavailable, it's almost always possible to accomplish with Deephaven.

## Time-based vs tick-based operations

[`updateby`](/core/javadoc/io/deephaven/api/updateby/package-summary.html) functions use a window of data that is measured either by ticks (number of rows) or by specifying a time window. When using tick-based operators, the window is fixed in size and contains the data elements within the specified number of rows relative to the current row. Ticks can be specified as backward ticks (`rev_ticks`), forward ticks (`fwd_ticks`), or both. Here are some examples:

- `revTicks = 1, fwdTicks = 0` - Contains only the current row.
- `revTicks = 10, fwdTicks = 0` - Contains 9 previous rows and the current row.
- `revTicks = 0, fwdTicks = 10` - Contains the following 10 rows; excludes the current row.
- `revTicks = 10, fwdTicks = 10` - Contains the previous 9 rows, the current row and the 10 rows following.
- `revTicks = 10, fwdTicks = -5` - Contains 5 rows, beginning at 9 rows before, ending at 5 rows before the current row (inclusive).
- `revTicks = 11, fwdTicks = -1` - Contains 10 rows, beginning at 10 rows before, ending at 1 row before the current row (inclusive).
- `revTicks = -5, fwdTicks = 10` - Contains 5 rows, beginning 5 rows following, ending at 10 rows following the current row (inclusive).

For time-based operators, a timestamp column must be specified. The window will contain the data elements within the specified time period of the current row's timestamp. The number of rows in a time-based window will not be fixed and may actually be empty depending on the sparsity of the data. Time can be specified in terms of backward-looking time (`revTime`), forward-looking time (`fwdTime`), or both. Here are some examples:

- `revTime = "PT00:00:00", fwdTime = "PT00:00:00"` - Contains rows that exactly match the current timestamp.
- `revTime = "PT00:10:00", fwdTime = "PT00:00:00"` - Contains rows from 10m earlier through the current timestamp (inclusive).
- `revTime = "PT00:00:00", fwdTime = "PT00:10:00"` - Contains rows from the current timestamp through 10m following the current row timestamp (inclusive).
- `revTime = int(60e9), fwdTime = int(60e9)` - Contains rows from 1m earlier through 1m following the current timestamp (inclusive).
- `revTime = "PT00:10:00", fwdTime = "-PT00:05:00"` - Contains rows from 10m earlier through 5m before the current timestamp (inclusive). This is a purely backward-looking window.
- `revTime = int(-5e9), fwdTime = int(10e9)` - Contains rows from 5s following through 10s following the current timestamp (inclusive). This is a purely forward-looking window.

Cumulative operators like [`CumSum`](../reference/table-operations/update-by-operations/cum-sum.md) are special cases of tick-based operators, where the window begins at the first table row and continues through to the current row.

## Simple moving (rolling) statistics

Simple moving (or rolling) statistics are ordinary statistics computed over a moving data window. Here are the simple moving statistics that Deephaven supports and the [`updateby`](/core/javadoc/io/deephaven/api/updateby/package-summary.html) functions that implement them:

| Simple moving statistic | Tick-based                                                                             |
| ----------------------- | -------------------------------------------------------------------------------------- |
| Count                   | [`RollingCount`](../reference/table-operations/update-by-operations/rolling-count.md)  |
| Minimum                 | [`RollingMin`](../reference/table-operations/update-by-operations/rolling-min.md)      |
| Maximum                 | [`RollingMax`](../reference/table-operations/update-by-operations/rolling-max.md)      |
| Sum                     | [`RollingSum`](../reference/table-operations/update-by-operations/rolling-sum.md)      |
| Product                 | [`RollingProd`](../reference/table-operations/update-by-operations/rolling-product.md) |
| Average                 | [`RollingAvg`](../reference/table-operations/update-by-operations/rolling-avg.md)      |
| Weighted Average        | [`RollingWavg`](../reference/table-operations/update-by-operations/rolling-wavg.md)    |
| Standard Deviation      | [`RollingStd`](../reference/table-operations/update-by-operations/rolling-std.md)      |

Deephaven also offers the [`RollingGroup`](../reference/table-operations/update-by-operations/rolling-group.md) function for creating rolling groups, and the [`RollingFormula`](../reference/table-operations/update-by-operations/rolling-formula.md) function for implementing custom rolling operations using DQL.

### Rolling average

To illustrate a simple moving statistic, consider the simple moving average. It is the average of all data points inside of a given window, and that window moves across the dataset to generate the simple moving average for each row. Here is an illustration of a 4-tick moving average:

![Diagram illustrating how a rolling average is calculated](../assets/how-to/rolling-calculations-1.png)

The fourth element of the moving average column is the average of the first four data points, the fifth element is the average of the second through fifth data points, and so on.

This calculation is implemented in Deephaven using the [`RollingAvg`](../reference/table-operations/update-by-operations/rolling-avg.md) function and the [`updateBy`](../reference/table-operations/update-by-operations/updateBy.md) table operation:

```groovy order=result
source = emptyTable(8).update("X = ii")

result = source.updateBy(RollingAvg(4, "AvgX=X"))
```

When creating a tick-based rolling operation, the `revTicks` parameter can configure how far the window extends behind the row. The current row is considered to belong to the backward-looking window, so setting `revTicks=10` includes the current row and previous 9 rows.

The following example creates 2, 3, and 5-row backward-looking tick-based simple moving averages. These averages are computed _by group_, as specified with the `by` argument:

```groovy order=result
source = emptyTable(10).update("Letter = (i % 2 == 0) ? `A` : `B`", "X = randomInt(0, 100)")

sma2 = RollingAvg(2, "AvgX2=X")
sma3 = RollingAvg(3, "AvgX3=X")
sma5 = RollingAvg(5, "AvgX5=X")

result = source.updateBy([sma2, sma3, sma5], "Letter")
```

Here's another example that creates 3, 5, and 9-row windows, centered on the current row:

```groovy order=result
source = emptyTable(10).update("Letter = (i % 2 == 0) ? `A` : `B`", "X = randomInt(0, 100)")

// using fwdTicks gives windows that extend into the future
sma3 = RollingAvg(2, 1, "AvgX2=X")
sma5 = RollingAvg(3, 2, "AvgX3=X")
sma9 = RollingAvg(5, 4, "AvgX5=X")

result = source.updateBy([sma3, sma5, sma9], "Letter")
```

### Time-based rolling average

Time-based rolling operations use a syntax similar to tick-based operations but require a timestamp column to be specified. This example uses the [`RollingAvg`](../reference/table-operations/update-by-operations/rolling-avg.md) function to compute 2-second, 3-second, and 5-second moving averages:

```groovy order=result
source = emptyTable(10).update(
        "Timestamp = '2023-01-01T00:00:00 ET' + i * SECOND",
        "Letter = (i % 2 == 0) ? `A` : `B`",
        "X = randomInt(0, 25)",
)

sma2sec = RollingAvg("Timestamp", parseDuration("PT00:00:02"), "AvgX2Sec=X")
sma3sec = RollingAvg("Timestamp", parseDuration("PT3s"), "AvgX3Sec=X")
sma5sec = RollingAvg("Timestamp", parseDuration("PT3s"), "AvgX5Sec=X")

result = source.updateBy([sma2sec, sma3sec, sma5sec], "Letter")
```

Like before, you can use `fwdTime` to create windows into the future. Here's an example that creates 2-second, 5-second, and 10-second windows, centered on the current row:

```groovy order=result
source = emptyTable(10).update(
        "Timestamp = '2023-01-01T00:00:00 ET' + i * SECOND",
        "Letter = (i % 2 == 0) ? `A` : `B`",
        "X = randomInt(0, 25)",
)

sma2sec = RollingAvg("Timestamp", parseDuration("PT00:00:01"), parseDuration("PT00:00:01"), "AvgX2Sec=X")
sma5sec = RollingAvg("Timestamp", parseDuration("PT2.5s"), parseDuration("PT2.5s"), "AvgX3Sec=X")
sma10sec = RollingAvg("Timestamp", parseDuration("PT5s"), parseDuration("PT5s"), "AvgX5Sec=X")

result = source.updateBy([sma2sec, sma5sec, sma10sec], "Letter")
```

## Exponential moving statistics

Exponential moving statistics are another form of moving statistics that depart from the concept of a sliding window of data. Instead, these statistics utilize _all_ of the data that comes before a given data point, as cumulative statistics do. However, they place more weight on recent data points and down-weight distant ones. This means that distant observations have little effect on the moving statistic, while closer observations carry more weight. The larger the `decay_rate` parameter is, the more weight distant observations carry. Here are the exponential moving statistics that Deephaven supports and the [`updateby`](/core/javadoc/io/deephaven/api/updateby/package-summary.html) functions that implement them:

| Exponential moving statistic | Tick-based                                                              |
| ---------------------------- | ----------------------------------------------------------------------- |
| Minimum (EMMin)              | [`EmMin`](../reference/table-operations/update-by-operations/em-min.md) |
| Maximum (EMMax)              | [`EmMax`](../reference/table-operations/update-by-operations/em-max.md) |
| Sum (EMS)                    | [`Ems`](../reference/table-operations/update-by-operations/ems.md)      |
| Average (EMA)                | [`Ema`](../reference/table-operations/update-by-operations/ema.md)      |
| Standard Deviation (EMStd)   | [`EmStd`](../reference/table-operations/update-by-operations/em-std.md) |

### Tick-based exponential moving average

To illustrate an exponential moving statistic, consider the exponential moving average (EMA). Here's a visualization of the EMA:

![Diagram illustrating how an exponential moving average is calculated](../assets/how-to/rolling-calculations-2.png)

Each element in the new column depends on _every_ data point that came before it, but distant data points only have a very small effect. Check out the [reference documentation for `Ema`](../reference/table-operations/update-by-operations/ema.md) for the formula used to compute this statistic.

This calculation is implemented in Deephaven using the [`Ema`](../reference/table-operations/update-by-operations/ema.md) function and the [`updateBy`](../reference/table-operations/update-by-operations/updateBy.md) table operation:

```groovy order=result
source = emptyTable(8).update("X = ii")

result = source.updateBy(Ema(2, "EmaX=X"))
```

The following example shows how to create exponential moving averages with decay rates of 2, 3, and 5. These averages are computed _by group_, as specified with the `by` argument:

```groovy order=result
source = emptyTable(10).update("Letter = (i % 2 == 0) ? `A` : `B`", "X = randomInt(0, 100)")

ema2 = Ema(2, "EMAX2=X")
ema3 = Ema(3, "EMAX3=X")
ema5 = Ema(5, "EMAX5=X")

result = source.updateBy([ema2, ema3, ema5], "Letter")
```

### Time-based exponential moving average

Time-based exponential moving statistics are conceptually similar to tick-based exponential moving statistics but measure the distance between observations in terms of time rather than the number of rows between them. Check out the [reference documentation for `Ema`](../reference/table-operations/update-by-operations/ema.md) for the formula used to compute this statistic.

Here is an example similar to the time-based simple moving average that utilizes the EMA with decay times of 2 seconds, 3 seconds, and 5 seconds:

```groovy order=result
source = emptyTable(50).update(
        "Timestamp = '2023-01-01T00:00:00 ET' + i * SECOND",
        "Letter = (i % 2 == 0) ? `A` : `B`",
        "X = randomInt(0, 25)",
)

ema2sec = Ema("Timestamp", parseDuration("PT2s"), "EmaX2Sec=X")
ema3sec = Ema("Timestamp", parseDuration("PT00:00:03"), "EmaX3=X")
ema5sec = Ema("Timestamp", parseDuration("PT5s"), "EmaX5=X",)

result = source.updateBy([ema2sec, ema3sec, ema5sec], "Letter")
```

## Bollinger Bands

Bollinger bands are an application of moving statistics frequently used in financial applications.

To compute Bollinger Bands:

1. Compute the moving average.
2. Compute the moving standard deviation.
3. Compute the upper and lower envelopes.

### Tick-based Bollinger Bands using simple moving statistics

When computing tick-based Bollinger bands, [`updateBy`](../reference/table-operations/update-by-operations/updateBy.md), [`RollingAvg`](../reference/table-operations/update-by-operations/rolling-avg.md) and [`RollingStd`](../reference/table-operations/update-by-operations/rolling-std.md) are used to compute the average and envelope. Here, `revTicks` is the moving average decay rate in ticks and is used to specify the size of the rolling window.

```groovy order=fAbc,fXyz,source,result
// Generate some random example data

source = emptyTable(1000).update(
        "Timestamp='2023-01-13T12:00 ET' + i*MINUTE",
        "Ticker = i%2==0 ? `ABC` : `XYZ`",
        "Price = i%2==0 ? 100*sin(i/40)+100*random() : 100*cos(i/40)+100*random()+i/2",
)

// Compute the Bollinger Bands

revTicks = 20

// Coverage parameter - determines the width of the bands
w = 2

result = source.updateBy(
    [
        RollingAvg(revTicks, "AvgPrice=Price"),
        RollingAvg(revTicks, "StdPrice=Price"),
    ],
    "Ticker",
).update("Upper = AvgPrice + w*StdPrice", "Lower = AvgPrice - w*StdPrice")

// Plot the Bollinger Bands

def plotBollinger = { t, ticker ->
    d = t.where("Ticker=`${ticker}`")
    plot = plot("Price", d, "Timestamp", "Price")
        .plot("AvgPrice", d, "Timestamp", "AvgPrice")
        .plot("Upper", d, "Timestamp", "Upper")
        .plot("Lower", d, "Timestamp", "Lower")
        .show()
    return d
}


fAbc = plotBollinger(result, "ABC")
fXyz = plotBollinger(result, "XYZ")
```

### Time-based Bollinger Bands using simple moving statistics

When computing time-based Bollinger Bands, [`updateBy`](../reference/table-operations/update-by-operations/updateBy.md), [`RollingAvg`](../reference/table-operations/update-by-operations/rolling-avg.md) and [`RollingStd`](../reference/table-operations/update-by-operations/rolling-std.md) are used to compute the average and envelope. Here, `revTime` is the moving average window time.

```groovy order=fAbc,fXyz,source,result
// Generate some random example data

source = emptyTable(1000).update(
        "Timestamp='2023-01-13T12:00 ET' + i*MINUTE",
        "Ticker = i%2==0 ? `ABC` : `XYZ`",
        "Price = i%2==0 ? 100*sin(i/40)+100*random() : 100*cos(i/40)+100*random()+i/2"
)

// Compute the Bollinger Bands

revTime = "PT00:20:00"
// Coverage parameter - determines the width of the bands
w = 2

result = source.updateBy(
    [
        RollingAvg("Timestamp", parseDuration(revTime), "AvgPrice=Price"),
        RollingStd("Timestamp", parseDuration(revTime), "StdPrice=Price"),
    ],
    "Ticker",
).update("Upper = AvgPrice + w*StdPrice", "Lower = AvgPrice - w*StdPrice")

// Plot the Bollinger Bands

def plotBollinger = { t, ticker ->
    d = t.where("Ticker=`${ticker}`")
    plot = plot("Price", d, "Timestamp", "Price")
        .plot("AvgPrice", d, "Timestamp", "AvgPrice")
        .plot("Upper", d, "Timestamp", "Upper")
        .plot("Lower", d, "Timestamp", "Lower")
        .show()
    return d
}

fAbc = plotBollinger(result, "ABC")
fXyz = plotBollinger(result, "XYZ")
```

### Tick-based Bollinger Bands using exponential moving statistics

When computing tick-based Bollinger Bands, [`updateBy`](../reference/table-operations/update-by-operations/updateBy.md), [`Ema`](../reference/table-operations/update-by-operations/ema.md) and [`EmStd`](../reference/table-operations/update-by-operations/em-std.md) are used to compute the average and envelope. Here, `decayTicks` is the moving average decay rate in ticks and is used to specify the weighting of previous data points.

```groovy order=fAbc,fXyz,source,result
// Generate some random example data

source = emptyTable(1000).update(
        "Timestamp='2023-01-13T12:00 ET' + i*MINUTE",
        "Ticker = i%2==0 ? `ABC` : `XYZ`",
        "Price = i%2==0 ? 100*sin(i/40)+100*random() : 100*cos(i/40)+100*random()+i/2"
)

// Compute the Bollinger Bands

decayTicks = 20
// Coverage parameter - determines the width of the bands
w = 2

result = source.updateBy(
    [
        Ema(decayTicks, "EmaPrice=Price"),
        EmStd(decayTicks, "StdPrice=Price"),
    ],
    "Ticker",
).update("Upper = EmaPrice + w*StdPrice", "Lower = EmaPrice - w*StdPrice")

// Plot the Bollinger Bands

def plotBollinger = { t, ticker ->
    d = t.where("Ticker=`${ticker}`")
    plot = plot("Price", d, "Timestamp", "Price")
        .plot("AvgPrice", d, "Timestamp", "EmaPrice")
        .plot("Upper", d, "Timestamp", "Upper")
        .plot("Lower", d, "Timestamp", "Lower")
        .show()
    return d
}

fAbc = plotBollinger(result, "ABC")
fXyz = plotBollinger(result, "XYZ")
```

### Time-based Bollinger Bands using exponential moving statistics

When computing time-based Bollinger Bands, [`updateBy`](../reference/table-operations/update-by-operations/updateBy.md), [`Ema`](../reference/table-operations/update-by-operations/ema.md) and [`EmStd`](../reference/table-operations/update-by-operations/em-std.md) are used to compute the average and envelope. Here, `decayTime` is the moving average decay rate in time and is used to specify the weighting of new data points.

```groovy order=fAbc,fXyz,source,result
// Generate some random example data

source = emptyTable(1000).update(
        "Timestamp='2023-01-13T12:00 ET' + i*MINUTE",
        "Ticker = i%2==0 ? `ABC` : `XYZ`",
        "Price = i%2==0 ? 100*sin(i/40)+100*random() : 100*cos(i/40)+100*random()+i/2",
)

// Compute the Bollinger Bands

decayTime = "PT00:20:00"

// Coverage parameter - determines the width of the bands
w = 2

result = source.updateBy(
    [
        Ema("Timestamp", parseDuration(decayTime), "EmaPrice=Price"),
        Ema("Timestamp", parseDuration(decayTime), "StdPrice=Price"),
    ],
    "Ticker",
).update("Upper = EmaPrice + w*StdPrice", "Lower = EmaPrice - w*StdPrice")

// Plot the Bollinger Bands

def plotBollinger = { t, ticker ->
    d = t.where("Ticker=`${ticker}`")
    plot = plot("Price", d, "Timestamp", "Price")
        .plot("AvgPrice", d, "Timestamp", "EmaPrice")
        .plot("Upper", d, "Timestamp", "Upper")
        .plot("Lower", d, "Timestamp", "Lower")
        .show()
    return d
}



fAbc = plotBollinger(result, "ABC")
fXyz = plotBollinger(result, "XYZ")
```

## Related documentation

- [Create a time table](./time-table.md)
- [Create an empty table](./new-and-empty-table.md#emptytable)
- [How to create plots with the legacy API](./plotting/api-plotting.md)
- [How to use select, view, and update](./use-select-view-update.md)
- [How to use update_by](./use-update-by.md)
- [Handle nulls, infs, and NaNs](./handle-null-inf-nan.md)
- [`Ema`](../reference/table-operations/update-by-operations/ema.md)
- [`EmStd`](../reference/table-operations/update-by-operations/em-std.md)
- [Formulas](../how-to-guides/formulas-how-to.md)
