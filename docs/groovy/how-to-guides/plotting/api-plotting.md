---
title: Built-in plotting API
---

Deephaven's built-in plotting API offers methods for creating real-time figures, plots, and charts from static and ticking tables. This guide provides an overview of its capabilities.

> [!NOTE]
> The built-in plotting API is no longer under active development, and there are no plans for new patches, features, or fixes. [Deephaven Express](/core/plotly/docs/), the recommended choice for most plotting tasks, is available only in Python. From Groovy, the built-in plotting API is the way to plot Deephaven tables in code.

This guide covers the following plot types:

- [XY series plots](#xy-series)
- [Category plots](#category)
- [Category histograms](#category-histogram)
- [Histograms](#histogram)
- [OHLC plots](#ohlc)
- [Pie charts](#pie)
- [Treemaps](#treemaps)

It also shows how to [combine plots in subplots](#subplots) and [make plots that users filter from the UI](#dynamic-plots).

Each plotting method returns a [`Figure`](/core/javadoc/io/deephaven/plot/Figure.html). You can call a plotting method such as [`plot`](../../reference/plot/plot.md) on its own, as most examples on this page do, or on a figure created with [`figure`](/core/javadoc/io/deephaven/plot/PlottingConvenience.html#figure()). Either way, you chain more methods onto the result and finish with [`show`](/core/javadoc/io/deephaven/plot/Figure.html#show()).

## XY series

XY series plots generally show values over a continuum, such as time. They can take the form of a line, a bar, an area, or a collection of points. The X axis shows the domain, while the Y axis shows the related values at specific points in the range.

You can create XY series plots using data from Deephaven tables with the following syntax:

```groovy syntax
plot(seriesName, source, xCol, yCol).show()
```

- [`plot`](../../reference/plot/plot.md) is the method used to create an XY series plot.
- `seriesName` is the name (as a [string](../../reference/query-language/types/strings.md)) you want to use to identify the series on the plot itself.
- `source` is the table that holds the data you want to plot.
- `xCol` is the name of the column (as a [string](../../reference/query-language/types/strings.md)) that holds the X values.
- `yCol` is the name of the column (as a [string](../../reference/query-language/types/strings.md)) that holds the Y values.
- [`show`](/core/javadoc/io/deephaven/plot/Figure.html#show()) returns a displayable version of the figure. Assign the result to a variable to see the plot in the console.

The example below creates an XY series plot of distance traveled over time during a metric-century bike ride. The [`where`](../../reference/table-operations/filter/where.md) filter drops the rows where the rider was stopped.

```groovy test-set=1 order=source,plotSingle default=plotSingle
import static io.deephaven.csv.CsvTools.readCsv

// source the data
source = readCsv("https://media.githubusercontent.com/media/deephaven/examples/main/MetricCentury/csv/metriccentury.csv")

// plot the data
plotSingle = plot("Distance", source.where("SpeedKPH > 0"), "Time", "DistanceMeters").show()
```

### Shared axes

You can compare multiple series over the same period of time by creating an XY series plot with shared axes. In the following example, two series create two line graphs on the same plot.

```groovy test-set=1
plotSharedAxis = plot("Altitude", source, "Time", "AltitudeMeters")\
    .plot("Speed", source, "Time", "SpeedKPH")\
    .show()
```

To add more series to the plot, chain more [`plot`](../../reference/plot/plot.md) calls onto the figure.

> [!TIP]
> To hide a series, click its name at the right of the plot. Click the name again to restore it.

### Plot multiple series in a loop

It's common in data analysis to have multiple series that share an X axis. In that case, a `for` loop can add each series to the figure. The following example plots three different series on the same plot in a `for` loop.

```groovy test-set=2 order=loopSource,plotLoop default=plotLoop
loopSource = emptyTable(10).update("X = i", "Y1 = randomInt(1, 10)", "Y2 = randomInt(3, 11)", "Y3 = randomInt(5, 15)")

fig = figure()

for (idx in 1..3) {
    fig = fig.plot("Y${idx}", loopSource, "X", "Y${idx}")
}

plotLoop = fig.show()
```

### Plot by a grouping column

The [`plotBy`](../../reference/plot/plotBy.md) method plots a separate series for each value in a grouping column:

```groovy syntax
plotBy(seriesName, source, xCol, yCol, groupByCol).show()
```

```groovy test-set=2 order=source,plotByInstrument default=plotByInstrument
import static io.deephaven.csv.CsvTools.readCsv

// source the data
source = readCsv("https://media.githubusercontent.com/media/deephaven/examples/main/CryptoCurrencyHistory/CSV/CryptoTrades_20210922.csv")

// plot the data
plotByInstrument = plotBy("Sept22", source, "Timestamp", "Price", "Instrument").show()
```

Category and OHLC plots have equivalent methods, described in [Category: Plot by a grouping column](#plot-by-a-grouping-column-1) and [OHLC: Plot by a grouping column](#plot-by-a-grouping-column-2).

### Multiple X or Y axes

When series in one plot have very different value ranges, a shared Y axis has to cover all of them, so the smaller series flatten and their changes become hard to see.

To fix this, add a second Y axis with the [`twinX`](/core/javadoc/io/deephaven/plot/Axes.html#twinX()) method. [`twinX`](/core/javadoc/io/deephaven/plot/Axes.html#twinX()) enables you to use one Y axis for some of the series being plotted and a second Y axis for the others, while sharing the same X axis:

```groovy syntax
plotName = figure().plot(...).twinX().plot(...).show()
```

- Series plotted _before_ [`twinX`](/core/javadoc/io/deephaven/plot/Axes.html#twinX()) share the left Y axis.
- Series plotted _after_ [`twinX`](/core/javadoc/io/deephaven/plot/Axes.html#twinX()) share the right Y axis.
- All series share the same X axis.

In the following example, distance reaches about 100,000 meters while speed stays below 70 KPH, so each series gets its own Y axis:

```groovy test-set=1
plotSharedTwinX = plot("Distance", source, "Time", "DistanceMeters")\
    .twinX()\
    .plot("Speed", source, "Time", "SpeedKPH")\
    .show()
```

The left Y axis shows the value range for distance, and the right Y axis shows the value range for speed.

The [`twinY`](/core/javadoc/io/deephaven/plot/Axes.html#twinY()) method enables you to use one X axis for one set of the values being plotted and a second X axis for another, while sharing the same Y axis:

```groovy syntax
plotName = figure().plot(...).twinY().plot(...).show()
```

- Series plotted _before_ [`twinY`](/core/javadoc/io/deephaven/plot/Axes.html#twinY()) use the lower X axis.
- Series plotted _after_ [`twinY`](/core/javadoc/io/deephaven/plot/Axes.html#twinY()) use the upper X axis.

### Plot colors

Use [`lineColor`](/core/javadoc/io/deephaven/plot/Figure.html#lineColor(java.lang.String)) to set a series' line color and [`pointColor`](/core/javadoc/io/deephaven/plot/Figure.html#pointColor(java.lang.String)) to set its point color. Both accept a named color, such as `"RED"`, or a color you build from red, green, blue, and alpha values with [`colorRGB`](/core/javadoc/io/deephaven/gui/color/Color.html#colorRGB(int,int,int,int)). This example takes the Distance and Speed plot from [Multiple X or Y axes](#multiple-x-or-y-axes) and draws its lines in red and yellow:

```groovy test-set=1
plotSharedTwinXColors = plot("Distance", source, "Time", "DistanceMeters")\
    .lineColor("RED")\
    .twinX()\
    .plot("Speed", source, "Time", "SpeedKPH")\
    .lineColor("YELLOW")\
    .show()
```

For the full list of available colors, see [Named colors](../../reference/table-operations/format/named-colors.md).

### Plot styles

The XY series plot in Deephaven defaults to a line plot. Use the [`plotStyle`](../../reference/plot/plotStyle.md) method to format XY series plots as area charts, stacked area charts, bar charts, stacked bar charts, scatter charts, or step charts.

In any of the examples below, you can pass another style name to [`plotStyle`](../../reference/plot/plotStyle.md), such as `"area"`, `"stacked_area"`, or `"step"`. Style names are not case-sensitive.

#### XY series as a stacked area plot

The following example stacks cadence on top of heart rate:

```groovy test-set=1
plotStackedArea = plot("Heart_rate", source, "Time", "HeartRate")\
    .plot("Cadence", source, "Time", "Cadence")\
    .plotStyle("stacked_area")\
    .show()
```

#### XY series as a scatter plot

The example below draws two series as points and styles each one:

- [`plotStyle("scatter")`](../../reference/plot/plotStyle.md) draws each series as points.
- [`pointSize`](/core/javadoc/io/deephaven/plot/Figure.html#pointSize(double)), [`pointColor`](/core/javadoc/io/deephaven/plot/Figure.html#pointColor(java.lang.String)), and [`pointShape`](/core/javadoc/io/deephaven/plot/Figure.html#pointShape(java.lang.String)) set the size, color, and shape of each series' points.
- [`twinX`](/core/javadoc/io/deephaven/plot/Axes.html#twinX()) gives each series its own Y axis.
- [`chartTitle`](/core/javadoc/io/deephaven/plot/Figure.html#chartTitle(java.lang.String)) sets the chart's title.

```groovy test-set=1
plotXYScatter = plot("Speed", source, "Time", "SpeedKPH")\
    .plotStyle("scatter")\
    .pointSize(0.5)\
    .pointColor(colorRGB(0, 0, 255, 50))\
    .pointShape("circle")\
    .twinX()\
    .plot("Distance", source, "Time", "DistanceMeters")\
    .plotStyle("scatter")\
    .pointSize(0.8)\
    .pointColor(colorRGB(255, 0, 0, 100))\
    .pointShape("up_triangle")\
    .chartTitle("Speed and Distance")\
    .show()
```

#### XY series with markers on selected points

In the example below, a line plot of speed gets a marker at every point where the rider went faster than 40 KPH.

- The [`twin`](/core/javadoc/io/deephaven/plot/Axes.html#twin()) method adds a second set of axes that shares both the X and Y axes.
- [`plotStyle("scatter")`](../../reference/plot/plotStyle.md) then gives those axes the scatter style, so the series plotted on them appears as markers over the line.

```groovy test-set=1 order=plotXYMarkers
fastPoints = source.where("SpeedKPH > 40")

plotXYMarkers = plot("Speed", source, "Time", "SpeedKPH")\
    .twin()\
    .plotStyle("scatter")\
    .plot("Over 40 KPH", fastPoints, "Time", "SpeedKPH")\
    .show()
```

#### XY series as a step plot

In the example below, [`plotStyle("step")`](../../reference/plot/plotStyle.md) draws a step plot. The [`lineStyle`](/core/javadoc/io/deephaven/plot/Figure.html#lineStyle(io.deephaven.plot.LineStyle)) method applies a [`LineStyle`](/core/javadoc/io/deephaven/plot/LineStyle.html) object to the series. The inner call, [`lineStyle(3)`](/core/javadoc/io/deephaven/plot/PlottingConvenience.html#lineStyle(double)), is a different method that builds a [`LineStyle`](/core/javadoc/io/deephaven/plot/LineStyle.html) with a width of 3.

```groovy test-set=1
plotStep = plot("HeartRate", source, "Time", "HeartRate")\
    .plotStyle("step")\
    .lineStyle(lineStyle(3))\
    .show()
```

## Category

Use the [`catPlot`](../../reference/plot/catPlot.md) method to create category plots, which display data values from different discrete categories.

To plot data from a Deephaven table, use the following syntax:

```groovy syntax
catPlot(seriesName, source, categoryCol, valueCol).show()
```

- [`catPlot`](../../reference/plot/catPlot.md) is the method used to create a category plot.
- `seriesName` is the name (as a [string](../../reference/query-language/types/strings.md)) you want to use to identify the series on the plot itself.
- `source` is the table that holds the data you want to plot.
- `categoryCol` is the name of the column (as a [string](../../reference/query-language/types/strings.md)) that holds the categories.
- `valueCol` is the name of the column (as a [string](../../reference/query-language/types/strings.md)) that holds the values.
- [`show`](/core/javadoc/io/deephaven/plot/Figure.html#show()) returns a displayable version of the figure. Assign the result to a variable to see the plot in the console.

```groovy test-set=3 order=source,result default=result
source = newTable(
    stringCol("Categories", "A", "B", "C"),
    intCol("Values", 1, 3, 5)
)

result = catPlot("Categories Plot", source, "Categories", "Values")
    .chartTitle("Categories And Values")
    .show()
```

[`chartTitle`](/core/javadoc/io/deephaven/plot/Figure.html#chartTitle(java.lang.String)) sets the chart's title.

### Shared axes

You can compare multiple series across the same categories by creating a category plot with shared axes. The following example reuses `source` from the [Category](#category) example, adds a second table, `sourceTwo`, with the same `Categories` column, and plots both as bar graphs on one chart:

```groovy test-set=3 order=sourceTwo,result default=result
sourceTwo = newTable(
    stringCol("Categories", "A", "B", "C"),
    intCol("Values", 2, 4, 6)
)

result = catPlot("Categories Plot One", source, "Categories", "Values")
    .catPlot("Categories Plot Two", sourceTwo, "Categories", "Values")
    .chartTitle("Categories And Values")
    .show()
```

To add more series to the chart, chain more [`catPlot`](../../reference/plot/catPlot.md) calls onto the figure.

### Plot by a grouping column

The [`catPlotBy`](../../reference/plot/catPlotBy.md) method plots a separate category series for each value in a grouping column:

```groovy syntax
catPlotBy(seriesName, source, categoryCol, valueCol, groupByCol).show()
```

### Plot styles

By default, category plots draw values as vertical bars. Use the [`plotStyle`](../../reference/plot/plotStyle.md) method to draw them as stacked bars, a line, an area, or a stacked area instead.

In the example below, you can pass another style name to [`plotStyle`](../../reference/plot/plotStyle.md), such as `"area"` or `"stacked_area"`.

#### Category plot with stacked bars

The following example draws the two series from the [category shared axes example](#shared-axes-1) as stacked bars:

```groovy test-set=3 order=result
result = catPlot("Categories Plot One", source, "Categories", "Values")
    .catPlot("Categories Plot Two", sourceTwo, "Categories", "Values")
    .plotStyle("stacked_bar")
    .chartTitle("Categories And Values")
    .show()
```

## Category histogram

Use the [`catHistPlot`](../../reference/plot/catHistPlot.md) method to create category histograms, which show how often each discrete value (category) occurs.

To plot data from a Deephaven table, use the following syntax:

```groovy syntax
catHistPlot(seriesName, source, valueCol).show()
```

- [`catHistPlot`](../../reference/plot/catHistPlot.md) is the method used to create a category histogram.
- `seriesName` is the name (as a [string](../../reference/query-language/types/strings.md)) you want to use to identify the series on the chart itself.
- `source` is the table that holds the data you want to plot.
- `valueCol` is the name of the column (as a [string](../../reference/query-language/types/strings.md)) containing the discrete values.
- [`show`](/core/javadoc/io/deephaven/plot/Figure.html#show()) returns a displayable version of the figure. Assign the result to a variable to see the plot in the console.

```groovy test-set=4 order=source,result default=result
source = newTable(
    intCol("Keys", 3, 2, 2, 1, 1, 1)
)

result = catHistPlot("Keys Count", source, "Keys")
    .chartTitle("Count Of Each Key")
    .show()
```

To plot data from an array, use the following syntax:

```groovy syntax
catHistPlot(seriesName, values).show()
```

- `values` is the array containing the discrete values.

```groovy test-set=5 order=result
def values = [3, 2, 2, 1, 1, 1] as int[]

result = catHistPlot("Values", values)
    .chartTitle("Count Of Values")
    .show()
```

## Histogram

Use the [`histPlot`](../../reference/plot/histPlot.md) method to create histograms.

A histogram shows how frequently different data values occur. It divides the data into intervals (bins), counts the values in each bin, and draws each count as a vertical bar. Unlike bar charts (category plots), bars in histograms do not have spaces between them unless there is a gap in the data.

To plot data from a Deephaven table, use the following syntax:

```groovy syntax
histPlot(seriesName, source, valueCol, nbins).show()
```

- [`histPlot`](../../reference/plot/histPlot.md) is the method used to create a histogram.
- `seriesName` is the name (as a [string](../../reference/query-language/types/strings.md)) you want to use to identify the series on the chart itself.
- `source` is the table that holds the data you want to plot.
- `valueCol` is the name of the column (as a [string](../../reference/query-language/types/strings.md)) that holds the X values.
- `nbins` is the number of intervals to use in the chart.
- [`show`](/core/javadoc/io/deephaven/plot/Figure.html#show()) returns a displayable version of the figure. Assign the result to a variable to see the plot in the console.

```groovy test-set=6 order=source,result default=result
source = newTable(
    intCol("Values", 1, 2, 2, 3, 3, 3, 4, 4, 5)
)

result = histPlot("Histogram Values", source, "Values", 5)
    .chartTitle("Histogram Of Values")
    .show()
```

The [`histPlot`](../../reference/plot/histPlot.md) method assumes you want to plot the entire range of values in the dataset. You can also set the minimum and maximum values of the range with `rangeMin` and `rangeMax`, respectively:

```groovy syntax
histPlot(seriesName, source, valueCol, rangeMin, rangeMax, nbins).show()
```

- `rangeMin` is the smallest value (as a double) to include.
- `rangeMax` is the largest value (as a double) to include.

```groovy test-set=6 order=result
result = histPlot("Histogram Values", source, "Values", 2.0, 4.0, 5)
    .chartTitle("Histogram Of Values")
    .show()
```

To plot data from an array, use the following syntax:

```groovy syntax
histPlot(seriesName, x, nbins).show()
```

- `x` is the array that holds the X values.

```groovy test-set=7 order=result default=result
values = [1, 2, 2, 3, 3, 3, 4, 4, 5] as int[]

result = histPlot("Histogram Values", values, 5)
    .chartTitle("Histogram Of Values")
    .show()
```

As with a table, you can set the minimum and maximum values of the range with `rangeMin` and `rangeMax`:

```groovy syntax
histPlot(seriesName, x, rangeMin, rangeMax, nbins).show()
```

```groovy test-set=7 order=result
result = histPlot("Histogram Values", values, 2.0, 4.0, 5)
    .chartTitle("Histogram Of Values")
    .show()
```

## OHLC

Use the [`ohlcPlot`](../../reference/plot/ohlcPlot.md) method to create Open, High, Low, and Close (OHLC) plots. These typically show four prices of a security or commodity per time slice: the open and close of the time slice, and the highest and lowest values reached during the time slice.

This plotting method requires a dataset that includes one column containing the values for the X axis (time), and one column for each of the corresponding four values (open, high, low, close).

To plot data from a Deephaven table, use the following syntax:

```groovy syntax
ohlcPlot(seriesName, source, timeCol, openCol, highCol, lowCol, closeCol).show()
```

- [`ohlcPlot`](../../reference/plot/ohlcPlot.md) is the method used to create an OHLC chart.
- `seriesName` is the name (as a [string](../../reference/query-language/types/strings.md)) you want to use to identify the series on the chart itself.
- `source` is the table that holds the data you want to plot.
- `timeCol` is the name of the column (as a [string](../../reference/query-language/types/strings.md)) that holds the X-axis values.
- `openCol` is the name of the column (as a [string](../../reference/query-language/types/strings.md)) holding the opening price.
- `highCol` is the name of the column (as a [string](../../reference/query-language/types/strings.md)) holding the highest price.
- `lowCol` is the name of the column (as a [string](../../reference/query-language/types/strings.md)) holding the lowest price.
- `closeCol` is the name of the column (as a [string](../../reference/query-language/types/strings.md)) holding the closing price.
- [`show`](/core/javadoc/io/deephaven/plot/Figure.html#show()) returns a displayable version of the figure. Assign the result to a variable to see the plot in the console.

The following example bins BTC trades into one-minute intervals and uses an [aggregation](../../reference/table-operations/group-and-aggregate/aggBy.md) to compute each interval's open, high, low, and close prices. The trades in the file are not in time order, so the example [sorts](../../reference/table-operations/sort/sort.md) them by `Timestamp` first. That way, [`AggFirst`](../../reference/table-operations/group-and-aggregate/AggFirst.md) and [`AggLast`](../../reference/table-operations/group-and-aggregate/AggLast.md) return each interval's earliest and latest trade.

```groovy test-set=8 order=cryptoTrades,btcBin,tOHLC,plotOHLC default=plotOHLC
import static io.deephaven.csv.CsvTools.readCsv

cryptoTrades = readCsv("https://media.githubusercontent.com/media/deephaven/examples/main/CryptoCurrencyHistory/CSV/CryptoTrades_20210922.csv")

aggList = [
    AggFirst("Open = Price"),
    AggMax("High = Price"),
    AggMin("Low = Price"),
    AggLast("Close = Price"),
]

btcBin = cryptoTrades.where("Instrument == `BTC/USD`")
    .update("TimestampBin = lowerBin(Timestamp, MINUTE)")
    .sort("Timestamp")

tOHLC = btcBin.aggBy(aggList, "TimestampBin")

plotOHLC = ohlcPlot("BTC", tOHLC, "TimestampBin", "Open", "High", "Low", "Close")\
    .chartTitle("BTC OHLC - Sep 22 2021")\
    .show()
```

### Plot by a grouping column

The [`ohlcPlotBy`](/core/javadoc/io/deephaven/plot/PlottingConvenience.html#ohlcPlotBy(java.lang.Comparable,io.deephaven.engine.table.Table,java.lang.String,java.lang.String,java.lang.String,java.lang.String,java.lang.String,java.lang.String...)) method plots a separate OHLC series for each value in a grouping column:

```groovy syntax
ohlcPlotBy(seriesName, source, timeCol, openCol, highCol, lowCol, closeCol, groupByCol).show()
```

The following example bins BTC and ETH trades together, computes OHLC values for each instrument and interval, and plots one series per instrument:

```groovy test-set=8 order=plotOHLCBy,tOHLCBy
// Bin the BTC and ETH trades into one-minute intervals
twoCoinsBin = cryptoTrades.where("Instrument in `BTC/USD`, `ETH/USD`")
    .update("TimestampBin = lowerBin(Timestamp, MINUTE)")
    .sort("Timestamp")

// Compute OHLC values per instrument and interval
tOHLCBy = twoCoinsBin.aggBy(aggList, "Instrument", "TimestampBin")

// Plot one OHLC series for each instrument
plotOHLCBy = ohlcPlotBy("Crypto", tOHLCBy, "TimestampBin", "Open", "High", "Low", "Close", "Instrument")\
    .show()
```

All the series share one Y axis. ETH trades at about one-fourteenth of BTC's price, so its series looks flat next to BTC's. To give each series its own Y axis, plot them separately, as the next section shows.

### Multiple series with separate Y axes

Like XY series plots, OHLC plots can show multiple series on one chart. The following example repeats the BTC binning from the [OHLC example](#ohlc), adds a second series for ETH, and uses [`twinX`](/core/javadoc/io/deephaven/plot/Axes.html#twinX()) to give ETH its own Y axis:

```groovy test-set=8 order=btcBin,ethBin,btcOHLC,ethOHLC,plotOHLC default=plotOHLC
btcBin = cryptoTrades.where("Instrument == `BTC/USD`")
    .update("TimestampBin = lowerBin(Timestamp, MINUTE)")
    .sort("Timestamp")
ethBin = cryptoTrades.where("Instrument == `ETH/USD`")
    .update("TimestampBin = lowerBin(Timestamp, MINUTE)")
    .sort("Timestamp")

aggList = [
    AggFirst("Open = Price"),
    AggMax("High = Price"),
    AggMin("Low = Price"),
    AggLast("Close = Price"),
]

btcOHLC = btcBin.aggBy(aggList, "TimestampBin")

ethOHLC = ethBin.aggBy(aggList, "TimestampBin")

plotOHLC = ohlcPlot("BTC", btcOHLC, "TimestampBin", "Open", "High", "Low", "Close")\
    .chartTitle("BTC and ETH OHLC - Sep 22 2021")\
    .twinX()\
    .ohlcPlot("ETH", ethOHLC, "TimestampBin", "Open", "High", "Low", "Close")\
    .show()
```

The second [`ohlcPlot`](../../reference/plot/ohlcPlot.md) call adds an ETH series built the same way as the BTC series. The [`twinX`](/core/javadoc/io/deephaven/plot/Axes.html#twinX()) call between them gives ETH its own Y axis, so the left Y axis shows the BTC price scale and the right Y axis shows the ETH price scale.

## Pie

Use the [`piePlot`](../../reference/plot/piePlot.md) method to create pie charts, which show each category's share of the total as a slice of a circle.

To create a pie chart from a table, use the following syntax:

```groovy syntax
piePlot(seriesName, source, categoryCol, valueCol).show()
```

- [`piePlot`](../../reference/plot/piePlot.md) is the method used to create a pie chart.
- `seriesName` is the name (as a [string](../../reference/query-language/types/strings.md)) you want to use to identify the series on the plot itself.
- `source` is the table that holds the data you want to plot.
- `categoryCol` is the name of the column (as a [string](../../reference/query-language/types/strings.md)) that holds the categories.
- `valueCol` is the name of the column (as a [string](../../reference/query-language/types/strings.md)) that holds the values.
- [`show`](/core/javadoc/io/deephaven/plot/Figure.html#show()) returns a displayable version of the figure. Assign the result to a variable to see the plot in the console.

```groovy test-set=9 order=totalInsurance,regionInsurance,pieChart default=pieChart
import static io.deephaven.csv.CsvTools.readCsv

totalInsurance = readCsv("https://media.githubusercontent.com/media/deephaven/examples/main/Insurance/csv/insurance.csv")

regionInsurance = totalInsurance.view("region", "expenses").sumBy("region")

pieChart = piePlot("Expense per region", regionInsurance, "region", "expenses")\
    .chartTitle("Expenses per region")\
    .show()
```

## Treemaps

Use the [`treemapPlot`](/core/javadoc/io/deephaven/plot/PlottingConvenience.html#treemapPlot(java.lang.Comparable,io.deephaven.engine.table.Table,java.lang.String,java.lang.String,java.lang.String,java.lang.String,java.lang.String,java.lang.String)) method to create treemap charts, which show hierarchical data as nested rectangles, where each item's area is proportional to its value.

### From a formatted table

Your table may already have the structure [`treemapPlot`](/core/javadoc/io/deephaven/plot/PlottingConvenience.html#treemapPlot(java.lang.Comparable,io.deephaven.engine.table.Table,java.lang.String,java.lang.String,java.lang.String,java.lang.String,java.lang.String,java.lang.String)) requires. It needs at least two columns:

- An ID column, which identifies each item.
- A parent column, which names the item each one sits under.

Following the parents up from any item must end at one top-level value, called the root. The root doesn't need a row of its own. In the examples below, `root` appears only in the parent column. When your table has this structure, use the following syntax:

```groovy syntax
treemapPlot(seriesName, source, ids, parents, values, labels, hoverTexts, colors).show()
```

- `seriesName` is the name (as a [string](../../reference/query-language/types/strings.md)) you want to use to identify the series on the plot itself.
- `source` is the table that holds the data you want to plot.
- `ids` is the name of the column that holds the item IDs to use in structuring the treemap chart.
- `parents` is the name of the column that holds each item's parent ID.
- `values` is the name of the column that sets how large each item is compared to the others.
- `labels` is the name of the column that holds the display text to use instead of the raw ID.
- `hoverTexts` is the name of the column that holds the text to display when hovering over an item.
- `colors` is the name of the column that holds the color to use for each item.
- [`show`](/core/javadoc/io/deephaven/plot/Figure.html#show()) returns a displayable version of the figure. Assign the result to a variable to see the plot in the console.

Only `ids` and `parents` are required. Pass `null` for any of the other columns you don't have. By default, each item takes its color from the theme, and its label is its ID.

The following example plots a table that already has this structure:

```groovy test-set=13 order=source,sAndP default=sAndP
import static io.deephaven.csv.CsvTools.readCsv

// source the data
source = readCsv("https://media.githubusercontent.com/media/deephaven/examples/main/StandardAndPoors500Financials/csv/s_and_p_500_market_cap.csv")

// plot the data
sAndP = treemapPlot("S&P 500 Companies by Sector", source, "Label", "Parent", null, null, null, null).show()
```

You can interact with this plot. Click a group header to expand that group, and click it again to return to the full view. Click an individual cell to expand its contents.

The same table also has value, hover text, and color columns. The following example uses them to display more information:

```groovy test-set=13 order=sAndPMarketCap
sAndPMarketCap = treemapPlot("S&P 500 Market Cap", source, "Label", "Parent", "Value", "Label", "HoverText", "Color").show()
```

The table's `Text` column holds each stock's percent change rather than a name, so the example passes the ID column, `Label`, as `labels` too. With a column of display names, pass that column instead.

### From a raw data table

Raw data often lacks the ID and parent columns that [`treemapPlot`](/core/javadoc/io/deephaven/plot/PlottingConvenience.html#treemapPlot(java.lang.Comparable,io.deephaven.engine.table.Table,java.lang.String,java.lang.String,java.lang.String,java.lang.String,java.lang.String,java.lang.String)) needs. The following example builds them with [`view`](../../reference/table-operations/select/view.md), [`selectDistinct`](../../reference/table-operations/select/select-distinct.md), and [`merge`](../../reference/table-operations/merge/merge.md):

```groovy test-set=14 order=sAndPTreemap,source,financials,sectors,sectorItems,stockItems,items
import static io.deephaven.csv.CsvTools.readCsv

// Source the data
source = readCsv("https://media.githubusercontent.com/media/deephaven/examples/main/StandardAndPoors500Financials/csv/s_and_p_500_companies_financials.csv")

// Get a view of just the data we need
financials = source.view("Symbol", "Name", "Sector", "MarketCap = Market_Cap")

// Find the distinct sectors defined in the data
sectors = financials.selectDistinct("Sector").sort("Sector")

// Define `root` as the top parent, use sectors as the first level of children
sectorItems = sectors.view(
    "Parent = `root`",
    "Label = Sector",
    "Value = (long)0",
    "Color = `#373438`",
    "HoverText = Sector"
)

// Update our data with random percent changes for display purposes, and color each stock by its change
stockItems = financials.update(
    "Perc = randomGaussian(0, 2) / 100.0",
    "PercLabel = String.format(`%.1f%%`, Perc * 100)"
).view(
    "Parent = Sector",
    "Label = Symbol",
    "Value = (long)MarketCap",
    "Color = Perc < -0.02 ? `#f95d84` : Perc < -0.01 ? `#f37e3f` : Perc > 0.01 ? `#9edc6f` : `#fcd65b`",
    "HoverText = Name + `: ` + PercLabel"
)

// Merge the topmost sectors with the rest of the stock items
items = merge(sectorItems, stockItems)

// Plot the data
sAndPTreemap = treemapPlot("S&P 500 Daily Change", items, "Label", "Parent", "Value", "Label", "HoverText", "Color").show()
```

- `financials` is a [view](../../reference/table-operations/select/view.md) of the raw `source` table with only the `Symbol`, `Name`, `Sector`, and `MarketCap` columns.
- `sectors` is a table of the top-level distinct sectors in the raw data.
- `sectorItems` is a table of the top-level sectors with label and color information associated with them.
- `stockItems` is a table with all the stock items and their associated labels. Its `Color` formula picks a color based on each stock's percent change.
- `items` [merges](../../reference/table-operations/merge/merge.md) `sectorItems` and `stockItems` into the single table that [`treemapPlot`](/core/javadoc/io/deephaven/plot/PlottingConvenience.html#treemapPlot(java.lang.Comparable,io.deephaven.engine.table.Table,java.lang.String,java.lang.String,java.lang.String,java.lang.String,java.lang.String,java.lang.String)) plots.
- `sAndPTreemap` is a treemap plot created with the [`treemapPlot`](/core/javadoc/io/deephaven/plot/PlottingConvenience.html#treemapPlot(java.lang.Comparable,io.deephaven.engine.table.Table,java.lang.String,java.lang.String,java.lang.String,java.lang.String,java.lang.String,java.lang.String)) method.

## Subplots

A figure can contain subplots: separate charts arranged in a grid by row and column number.

- A single plot with several series draws them all in one plot area, sharing at least one axis.
- Subplots give each chart its own section of the figure.

Use subplots when the datasets have different units or scales and you don't need to read them against each other. To overlay two series with different scales on one plot instead, see [Multiple X or Y axes](#multiple-x-or-y-axes).

### Define subplots

Set the number of subplots when you create a figure with [`figure`](/core/javadoc/io/deephaven/plot/PlottingConvenience.html#figure(int,int)). The arguments are the number of rows and the number of columns.

```groovy syntax
f = figure(numRows, numCols)
```

For a figure with only one plot, specifying the number of rows and columns is optional. A figure created with [`figure`](/core/javadoc/io/deephaven/plot/PlottingConvenience.html#figure()) and no arguments grows to fit each chart added by a [`newChart`](/core/javadoc/io/deephaven/plot/Figure.html#newChart()) call with no arguments.

### Add subplots to a figure

After defining the figure's grid, add each subplot with a [`newChart`](/core/javadoc/io/deephaven/plot/Figure.html#newChart(int,int)) call. Each [`newChart`](/core/javadoc/io/deephaven/plot/Figure.html#newChart(int,int)) call creates a chart at a given row and column in the figure, and the plotting methods that follow it draw on that chart. Row and column indices start at 0.

For example, to stack two subplots vertically, give the figure two rows and one column.

```groovy test-set=11 order=source,f default=f
source = emptyTable(100).update("X = i", "Y = cos(0.1 * i)", "Z = sin(0.1 * i)")

f = figure(2, 1)
    .newChart(0, 0)
    .plot("Y", source, "X", "Y")
    .newChart(1, 0)
    .plot("Z", source, "X", "Z")
    .show()
```

### One row, two columns

Let's instead place the subplots side by side by giving the figure one row and two columns. The series `Y` appears on the left, and `Z` on the right.

```groovy test-set=11 order=f
f = figure(1, 2)
    .newChart(0, 0)
    .plot("Y", source, "X", "Y")
    .newChart(0, 1)
    .plot("Z", source, "X", "Z")
    .show()
```

### Different plot types

A single figure can handle multiple plot types as well as both static and real-time data.

```groovy test-set=12 ticking-table order=null
t1 = timeTable("PT0.1S").updateView("X = 0.1 * i", "Y = sin(X)")

t2 = newTable(
    stringCol("Categories", "A", "B", "C"),
    intCol("Values", 1, 3, 5)
)

plotStaticAndRt = figure(2, 1)
    .newChart(0, 0)
    .catPlot("Categories", t2, "Categories", "Values")
    .newChart(1, 0)
    .plot("Sine", t1, "X", "Y")
    .show()
```

## Dynamic plots

A dynamic plot redraws to show only the rows that match the value a user picks in a paired filter control: Deephaven's [Input Filter](../user-interface/filters.md#input-filters), [Linker](../user-interface/filters.md#linker), or [Dropdown Filter](../user-interface/filters.md#dropdown-filter). To make a plot dynamic, use a [`SelectableDataSetOneClick`](../../reference/query-language/types/SelectableDataSetOneClick.md) as its data source.

### `oneClick`

The [`oneClick`](../../reference/plot/oneClick.md) method creates a [`SelectableDataSetOneClick`](../../reference/query-language/types/SelectableDataSetOneClick.md) from a table or a [partitioned table](../partitioned-tables.md):

```groovy syntax
oneClick(pTable)
oneClick(pTable, requireAllFiltersToDisplay)
oneClick(t, requireAllFiltersToDisplay, byColumns...)
oneClick(t, byColumns...)
```

- `pTable` is a [partitioned table](../partitioned-tables.md) containing the data. It must have at least one key column.
- `t` is the table containing the data.
- `byColumns` is one or more column names (as [strings](../../reference/query-language/types/strings.md)) to make available for input filtering. At least one column is required.
- `requireAllFiltersToDisplay` controls whether the plot waits for filters. When `true`, the plot displays data only after the user sets a filter on every `byColumns` column. When `false`, data displays before the user sets every filter. Overloads that omit this argument use `true`.

The following example uses [`oneClick(t, byColumns...)`](../../reference/plot/oneClick.md), so the plot shows no data until you pick an `Instrument` value. Until then, the plot displays a message with **Add Input Filters** and **Open Linker Tool** buttons:

<!-- skip-test: with requireAllFiltersToDisplay, the figure waits for an input filter, so the snapshotter times out serializing it. The screenshot below shows the expected result. -->

```groovy skip-test
import static io.deephaven.csv.CsvTools.readCsv

source = readCsv("https://media.githubusercontent.com/media/deephaven/examples/main/CryptoCurrencyHistory/CSV/CryptoTrades_20210922.csv")
oc = oneClick(source, "Instrument")
plotOneClick = plot("Plot", oc, "Timestamp", "Price").show()
```

![The plot waits for a filter on the Instrument column and shows the Add Input Filters and Open Linker Tool buttons](../../assets/how-to/oc-result-unfiltered.png)

### Input filters

To pair your dynamic plot with an [Input Filter](../user-interface/filters.md#input-filters), choose **Input Filter** from the **Controls** menu in the upper right-hand corner of the UI. If the plot is waiting for a filter, you can also click **Add Input Filters** on the plot itself.

![A user selects the Input Filter option from the Controls menu](../../assets/how-to/ui-input-filter-gr.gif)

> [!NOTE]
> The [Input Filter](../user-interface/filters.md#input-filters) feature filters every open table in the dashboard that has a column with the same name and type as the filter column. It also filters every dynamic plot whose [`SelectableDataSetOneClick`](../../reference/query-language/types/SelectableDataSetOneClick.md) filters on such a column. That column is either a column listed in `byColumns` or a key column of the [partitioned table](../partitioned-tables.md) passed to [`oneClick`](../../reference/plot/oneClick.md).

### Linker tool

As an alternative to an [Input Filter](../user-interface/filters.md#input-filters), you can use the [Linker](../user-interface/filters.md#linker) tool to pair a dynamic plot with a table. Choose **Linker** from the **Controls** menu. If the plot is waiting for a filter, you can also click **Open Linker Tool** on the plot.

![The Linker option highlighted in the Controls menu](../../assets/how-to/linker-button.png)

![A user pairs a dynamic plot with a table using the Linker tool](../../assets/how-to/linker-tool.gif)

In the [Linker](../user-interface/filters.md#linker), the table you filter from is the source table, and the dynamic plot is the target. Click the matching column in the source table, then click the column button on the dynamic plot. After that, double-clicking a row in the source table filters the plot to that row's value in the linked column.

### Dropdown filters

You can also pair a dynamic plot with a [Dropdown Filter](../user-interface/filters.md#dropdown-filter), which filters on a value you choose from a list:

1. Choose **Dropdown Filter** from the **Controls** menu.
2. Under **Source Column**, select a column to supply the list of values.
3. Under **Filter Column**, choose the column the plot's [`SelectableDataSetOneClick`](../../reference/query-language/types/SelectableDataSetOneClick.md) filters on.
4. Click **Save**, then pick a value from the list.

## Related documentation

- [How to use the Chart Builder](../user-interface/chart-builder.md)
- [Filter without writing queries](../user-interface/filters.md)
- [Create and use partitioned tables](../partitioned-tables.md)
- [Arrays](../../reference/query-language/types/arrays.md)
- [Named colors](../../reference/table-operations/format/named-colors.md)
- [`AggFirst`](../../reference/table-operations/group-and-aggregate/AggFirst.md)
- [`AggLast`](../../reference/table-operations/group-and-aggregate/AggLast.md)
- [`AggMax`](../../reference/table-operations/group-and-aggregate/AggMax.md)
- [`AggMin`](../../reference/table-operations/group-and-aggregate/AggMin.md)
- [`aggBy`](../../reference/table-operations/group-and-aggregate/aggBy.md)
- [`catHistPlot`](../../reference/plot/catHistPlot.md)
- [`catPlot`](../../reference/plot/catPlot.md)
- [`catPlotBy`](../../reference/plot/catPlotBy.md)
- [`emptyTable`](../../reference/table-operations/create/emptyTable.md)
- [`histPlot`](../../reference/plot/histPlot.md)
- [`intCol`](../../reference/table-operations/create/intCol.md)
- [`merge`](../../reference/table-operations/merge/merge.md)
- [`newTable`](../../reference/table-operations/create/newTable.md)
- [`ohlcPlot`](../../reference/plot/ohlcPlot.md)
- [`oneClick`](../../reference/plot/oneClick.md)
- [`piePlot`](../../reference/plot/piePlot.md)
- [`plot`](../../reference/plot/plot.md)
- [`plotBy`](../../reference/plot/plotBy.md)
- [`plotStyle`](../../reference/plot/plotStyle.md)
- [`readCsv`](../../reference/data-import-export/CSV/readCsv.md)
- [`selectDistinct`](../../reference/table-operations/select/select-distinct.md)
- [`SelectableDataSetOneClick`](../../reference/query-language/types/SelectableDataSetOneClick.md)
- [`sort`](../../reference/table-operations/sort/sort.md)
- [`stringCol`](../../reference/table-operations/create/stringCol.md)
- [`sumBy`](../../reference/table-operations/group-and-aggregate/sumBy.md)
- [`timeTable`](../../reference/table-operations/create/timeTable.md)
- [`update`](../../reference/table-operations/select/update.md)
- [`updateView`](../../reference/table-operations/select/update-view.md)
- [`view`](../../reference/table-operations/select/view.md)
- [`where`](../../reference/table-operations/filter/where.md)
- [`Axes` Javadoc](/core/javadoc/io/deephaven/plot/Axes.html)
- [`chartTitle` Javadoc](/core/javadoc/io/deephaven/plot/Figure.html#chartTitle(java.lang.String))
- [`colorRGB` Javadoc](/core/javadoc/io/deephaven/gui/color/Color.html#colorRGB(int,int,int,int))
- [`Figure` Javadoc](/core/javadoc/io/deephaven/plot/Figure.html)
- [`lineColor` Javadoc](/core/javadoc/io/deephaven/plot/Figure.html#lineColor(java.lang.String))
- [`lineStyle` Javadoc](/core/javadoc/io/deephaven/plot/Figure.html#lineStyle(io.deephaven.plot.LineStyle))
- [`LineStyle` Javadoc](/core/javadoc/io/deephaven/plot/LineStyle.html)
- [`newChart` Javadoc](/core/javadoc/io/deephaven/plot/Figure.html#newChart(int,int))
- [`ohlcPlotBy` Javadoc](/core/javadoc/io/deephaven/plot/PlottingConvenience.html#ohlcPlotBy(java.lang.Comparable,io.deephaven.engine.table.Table,java.lang.String,java.lang.String,java.lang.String,java.lang.String,java.lang.String,java.lang.String...))
- [`PlottingConvenience` Javadoc](/core/javadoc/io/deephaven/plot/PlottingConvenience.html)
- [`pointColor` Javadoc](/core/javadoc/io/deephaven/plot/Figure.html#pointColor(java.lang.String))
- [`pointShape` Javadoc](/core/javadoc/io/deephaven/plot/Figure.html#pointShape(java.lang.String))
- [`pointSize` Javadoc](/core/javadoc/io/deephaven/plot/Figure.html#pointSize(double))
- [`show` Javadoc](/core/javadoc/io/deephaven/plot/Figure.html#show())
- [`treemapPlot` Javadoc](/core/javadoc/io/deephaven/plot/PlottingConvenience.html#treemapPlot(java.lang.Comparable,io.deephaven.engine.table.Table,java.lang.String,java.lang.String,java.lang.String,java.lang.String,java.lang.String,java.lang.String))
- [`twin` Javadoc](/core/javadoc/io/deephaven/plot/Axes.html#twin())
- [`twinX` Javadoc](/core/javadoc/io/deephaven/plot/Axes.html#twinX())
- [`twinY` Javadoc](/core/javadoc/io/deephaven/plot/Axes.html#twinY())
