---
title: Built-in plotting API
---

Deephaven's built-in plotting API offers a robust set of methods for creating real-time figures, include XY series plots, bar charts, histograms, and pie charts. This guide provides an overview of the legacy plotting API and its capabilities.

Deephaven's legacy plotting API can be used to create various chart types:

- [XY series plots](#xy-series)
- [Category plots](#category)
- [Category histograms](#category-histogram)
- [Histograms](#histogram)
- [Pie charts](#pie)
- [OHLC plots](#ohlc)
- [Dynamic plots](#dynamic-plots)

> [!NOTE]
> [Deephaven Express](/core/plotly/docs/) is the recommended choice for most plotting tasks.

## XY series

XY series plots are generally used to show values over a continuum, such as time. XY Series plots can be represented as a line, a bar, an area or as a collection of points. The X axis is used to show the domain, while the Y axis shows the related values at specific points in the range.

You can create XY Series plots using data from Deephaven tables with the following syntax:

`.plot("SeriesName", source, "xCol", "yCol").show()`

- [`plot`](../../reference/plot/plot.md) is the method used to create an XY series plot.
- `"SeriesName"` is the name (as a [string](../../reference/query-language/types/strings.md)) you want to use to identify the series on the plot itself.
- `source` is the table that holds the data you want to plot.
- `"xCol"` is the name of the column of data to be used for the X value.
- `"yCol"` is the name of the column of data to be used for the Y value.
- `show` tells Deephaven to draw the plot in the console.

The example query below will create an XY series plot that shows the high of Bitcoin for September 8, 2021.

> [!NOTE]
> Python users must import the appropriate module: `from deephaven import Plot` or `from deephaven import Plot as plt`

```groovy test-set=1 order=source,plotSingle default=plotSingle
import static io.deephaven.csv.CsvTools.readCsv

//source the data
source = readCsv("https://media.githubusercontent.com/media/deephaven/examples/main/MetricCentury/csv/metriccentury.csv")

//plot the data
plotSingle = plot("Distance",  source.where("SpeedKPH > 0"), "Time", "DistanceMeters").show()
```

### Shared axes

You can compare multiple series over the same period of time by creating an XY series plot with shared axes. In the following example, two series are plotted, thereby creating two line graphs on the same plot.

```groovy test-set=1
plotSharedAxis = plot("Altitude", source, "Time", "AltitudeMeters")\
    .plot("Speed", source, "Time", "SpeedKPH")\
    .show()
```

> [!TIP]
> You can choose to hide one or more series in the plot. Simply click the name of the series at the right of the plot to hide that series; click the name again to restore it.

Subsequent series can be added to the plot by adding additional `plot` methods to the query.

### Multiple X or Y Axes

When plotting multiple series in a single plot, the range of the Y axis is an important factor to watch. As the range of the Y axis increases, value changes become harder to assess.

When the scale of the Y axis needs to cover an extremely wide range, the plot may result in relatively flat lines with barely distinguishable differences in values or trend.

This issue can be easily remedied by adding a second Y axis to the plot via the `twinX` method.

#### `twinX`

The `twinX` method enables you to use one Y axis for some of the series being plotted and a second Y axis for the others, while sharing the same X axis:

`PlotName = figure().plot(...).twinX().plot(...).show()`

- The plot(s) for the series placed _before_ the `twinX()` method share a common Y axis (on the left).
- The plot(s) for the series listed _after_ the `twinX()` method share a common Y axis (on the right).
- All plots share the same X axis.

```groovy test-set=1
plotSharedTwinX = plot("Altitude", source, "Time", "AltitudeMeters")\
    .twinX()\
    .plot("Speed", source, "Time", "SpeedKPH")\
    .show()
```

The value range for the high value is shown on the left axis and the value range for the low value is shown on the right axis.

#### `twinY`

The `twinY` method enables you to use one X axis for one set of the values being plotted and a second X axis for another, while sharing the same Y axis:

`PlotName = figure().plot(...).twinY().plot(...).show()`

- The plot(s) for the series placed _before_ the `twinY()` method use the lower X axis.
- The plot(s) for the series listed _after_ the `twinY()` method use the upper X axis.

### Multiple series

Multiple plot methods can be used within the same query to produce a chart with multiple series. However, the `plotBy` methods include an additional argument that enables users to specify the grouping column to be used to plot multiple series. This greatly simplifies and shortens the query structure:

```syntax
.plotBy("Series1", source, "xCol", "yCol", "groupByCol").show()

.catPlotBy("SeriesName", source, "CategoryCol", "ValueCol", "groupByCol").show()

.ohlcPlotBy("SeriesName", source, "Time", "Open", "High", "Low", "Close" "groupByCol").show()
```

```groovy test-set=2 order=source,plotBy default=plotBy
import static io.deephaven.csv.CsvTools.readCsv

//source the data
source = readCsv("https://media.githubusercontent.com/media/deephaven/examples/main/CryptoCurrencyHistory/CSV/CryptoTrades_20210922.csv")

//plot the data
plotBy = plotBy("Sept22", source, "Timestamp", "Price", "Instrument").show()
```

### Plot styles

The XY series plot in Deephaven defaults to a line plot. However, Deephaven's [`plotStyle`](../../reference/plot/plotStyle.md) method can be used to format XY series plots as area charts, stacked area charts, bar charts, stacked bar charts, scatter charts and step charts.

In any of the examples below, you can simply swap out the `plotStyle` argument with the appropriate name; e.g., `("area")`, `("stacked_area")`, `("step")`, etc.

#### XY Series as a stacked area plot

In any of the examples below, you can simply swap out the `plotStyle` argument with the name `("stacked_area")`, `("step")`, etc.

```groovy test-set=1
plotSingleStackedArea= plot("Heart_rate", source, "Time", "HeartRate").plotStyle("stacked_area")\
    .show()
```

#### XY Series as a scatter plot

In the example below, the `.plotStyle` argument has the name `("scatter")`. Other parameters are defined to show the fine tuning detail under control.

```groovy test-set=1
plotXYScatter = plot("Speed", source, "Time", "SpeedKPH")\
    .plotStyle("scatter")\
    .pointSize(0.5)\
    .pointColor(colorRGB(0,0,255,50))\
    .pointShape("circle")\
    .twinX()\
    .plot("Distance", source, "Time", "DistanceMeters")\
    .plotStyle("scatter")\
    .pointSize(0.8)\
    .pointColor(colorRGB(255,0,0,100))\
    .pointShape("up_triangle")\
    .chartTitle("Speed and Distance")\
    .show()
```

#### XY Series as a step plot

In the example below, the `.plotStyle` argument has the name `("step")`. Other parameters are defined to show the fine tuning detail under control.

```groovy test-set=1
plotStep = plot("HeartRate", source, "Time", "HeartRate")\
    .plotStyle("Step")\
    .lineStyle(lineStyle(3))\
    .show()
```

## Category

Use the [`catPlot`](../../reference/plot/catPlot.md) method to create category plots, which display data values from different discrete categories. Data may be sourced from a table or an array.

When data is sourced from a Deephaven table, the following syntax can be used:

`.catPlot(seriesName, source, categoryCol, valueCol).show()`

- `catPlot` is the method used to create a category plot.
- `seriesName` is the name ([string](../../reference/query-language/types/strings.md)) you want to use to identify the series on the plot itself.
- `source` is the table that holds the data you want to plot.
- `categoryCol` is the name of the column (as a [string](../../reference/query-language/types/strings.md)) to be used for the categories.
- `valueCol` is the name of the column (as a [string](../../reference/query-language/types/strings.md)) to be used for the values.
- `show` tells Deephaven to draw the plot in the console.

```groovy test-set=3 order=source,result default=result
source = newTable(
    stringCol("Categories", "A", "B", "C"),
    intCol("Values", 1, 3, 5)
)

result = catPlot("Categories Plot", source, "Categories", "Values")
    .chartTitle("Categories And Values")
    .show()
```

### Shared axes

You can also compare multiple categories over the same period of time by creating a category plot with shared axes. In the following example, a second category plot has been added to the previous example, thereby creating bar graphs on the same chart:

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

Subsequent categories can be added to the chart by adding additional [`catPlot`](../../reference/plot/catPlot.md) methods to the query.

### Plot styles

By default, values are presented as vertical bars. However, by using Deephaven's [`plotStyle`](../../reference/plot/plotStyle.md) method, the data can be represented as a bar, a stacked bar, a line, an area or a stacked area.

In any of the examples below, you can simply swap out the [`plotStyle`](../../reference/plot/plotStyle.md) argument with the appropriate name; e.g., `("Area")`, `("stacked_area")`, etc.

#### Category plot with Stacked Bar

```groovy test-set=3 order=result
result = catPlot("Categories Plot One", source, "Categories", "Values")
    .catPlot("Categories Plot Two", sourceTwo, "Categories", "Values")
    .plotStyle("stacked_bar")
    .chartTitle("Categories And Values")
    .show()
```

## Category histogram

Use the [`catHistPlot`](../../reference/plot/catHistPlot.md) method to create category histograms, which is used to show how frequently a set of discrete values (categories) occur.

When data is sourced from a Deephaven table, the following syntax can be used:

`.catHistPlot(seriesName, source, valueCol).show()`

- `catHistPlot` is the method used to create a category histogram.
- `seriesName` is the name (as a [string](../../reference/query-language/types/strings.md)) you want to use to identify the series on the chart itself.
- `source` is the table that holds the data you want to plot.
- `valueCol` is the name of the column (as a [string](../../reference/query-language/types/strings.md)) containing the discrete values.
- `show()` tells Deephaven to draw the plot in the console.

```groovy test-set=4 order=source,result default=result
source = newTable(
    intCol("Keys", 3, 2, 2, 1, 1, 1)
)

result = catHistPlot("Keys Count", source, "Keys")
    .chartTitle("Count Of Each Key")
    .show()
```

When data is sourced from an array, the following syntax can be used:

`.catHistPlot(seriesName, values).show()`

- `catHistPlot` is the method used to create a category histogram.
- `seriesName` is the name (as a [string](../../reference/query-language/types/strings.md)) you want to use to identify the series on the plot itself.
- `values` is the array containing the discrete values.
- `show` tells Deephaven to draw the plot in the console.

```groovy test-set=5 order=result
def values = [3, 2, 2, 1, 1, 1] as int[]

result = catHistPlot("Values", values)
    .chartTitle("Count Of Values")
    .show()
```

## Histogram

Use the [`histPlot`](../../reference/plot/histPlot.md) method to create histograms.

The histogram is used to show how frequently different data values occur. The data is divided into logical intervals (or bins) , which are then aggregated and charted with vertical bars. Unlike bar charts (category plots), bars in histograms do not have spaces between them unless there is a gap in the data.

When data is sourced from a table, the following syntax can be used:

`.histPlot(seriesName, source, valueCol, nbins).show()`

- `histPlot` is the method used to create a histogram.
- `seriesName` is the name (as a [string](../../reference/query-language/types/strings.md)) you want to use to identify the series on the chart itself.
- `source` is the table that holds the data you want to plot.
- `valueCol` is the name of the column (as a [string](../../reference/query-language/types/strings.md)) of data to be used for the X values.
- `nbins` is the number of intervals to use in the chart.
- `show` tells Deephaven to draw the plot in the console.

```groovy test-set=6 order=source,result default=result
source = newTable(
    intCol("Values", 1, 2, 2, 3, 3, 3, 4, 4, 5)
)

result = histPlot("Histogram Values", source, "Values", 5)
    .chartTitle("Histogram Of Values")
    .show()
```

The [`histPlot`](../../reference/plot/histPlot.md) method assumes you want to plot the entire range of values in the dataset. However, you can also set the minimum and maximum values of the range using `rangeMin` and `rangeMax` respectively:

`.histPlot(seriesName, source, valueCol, rangeMin, rangeMax, nbins).show()`

- `histPlot` is the method used to create a histogram.
- `seriesName` is the name (as a [string](../../reference/query-language/types/strings.md)) you want to use to identify the series on the chart itself.
- `source` is the table that holds the data you want to plot.
- `valueCol` is the name of the column (as a [string](../../reference/query-language/types/strings.md)) of data to be used for the X values.
- `rangeMin` is the minimum value (as a double) of the range to be included.
- `rangeMax` is the maximum value (as a double) of the range to be included.
- `nbins` is the number of intervals to use in the chart.
- `show` tells Deephaven to draw the plot in the console.

```groovy test-set=6 order=result
result = histPlot("Histogram Values", source, "Values", 2, 4, 5)
    .chartTitle("Histogram Of Values")
    .show()
```

When data is sourced from an array, the following syntax can be used:

`.histPlot(seriesName, x, nbins).show()`

- `histPlot` is the method used to create a histogram.
- `seriesName` is the name (as a [string](../../reference/query-language/types/strings.md)) you want to use to identify the series on the chart itself.
- `x` is the array containing the data to be used for the X values.
- `nbins` is the number of intervals to use in the chart.
- `show` tells Deephaven to draw the plot in the console.

```groovy test-set=7 order=result default=result
source = [1, 2, 2, 3, 3, 3, 4, 4, 5] as int[]

result = histPlot("Histogram Values", source, 5)
    .chartTitle("Histogram Of Values")
    .show()
```

Just like with a Deephaven table, you can also set the minimum and maximum values of the range using `rangeMin` and `rangeMax` respectively:

`.histPlot(seriesName, x, rangeMin, rangeMax, nbins).show()`

- `histPlot` is the method used to create a histogram.
- `seriesName` is the name (as a [string](../../reference/query-language/types/strings.md)) you want to use to identify the series on the chart itself.
- `x` is the array containing the data to be used for the X values.
- `rangeMin` is the minimum value (as a double) of the range to be included.
- `rangeMax` is the maximum value (as a double) of the range to be included.
- `nbins` is the number of the intervals to use in the chart.
- `show` tells Deephaven to draw the plot in the console.

```groovy test-set=7 order=result
result = histPlot("Histogram Values", source, 2, 4, 5)
    .chartTitle("Histogram Of Values")
    .show()
```

## OHLC

Use the [`ohlcPlot`](../../reference/plot/ohlcPlot.md) method to create Open, High, Low and Close (OHLC) plots. These typically shows four prices of a security or commodity per time slice: the open and close of the time slice, and the highest and lowest values reached during the time slice.

This plotting method requires a dataset that includes one column containing the values for the X axis (time), and one column for each of the corresponding four values (open, high, low, close).

When data is sourced from a table, the following syntax can be used:

`.ohlcPlot("SeriesName", source, "X", "Open", "High", "Low", "Close").show()`

- `ohlcPlot` is the method used to create an OHLC chart.
- `"SeriesName"` is the name (as a [string](../../reference/query-language/types/strings.md)) you want to use to identify the series on the chart itself.
- `source` is the table that holds the data you want to plot.
- `"X"` is the name (as a [string](../../reference/query-language/types/strings.md)) of the column to be used for the X axis.
- `"Open"` is the name of the column (as a [string](../../reference/query-language/types/strings.md)) holding the opening price.
- `"High"` is the name of the column (as a [string](../../reference/query-language/types/strings.md)) holding the highest price.
- `"Low"` is the name of the column (as a [string](../../reference/query-language/types/strings.md)) holding the lowest price.
- `"Close"` is the name of the column (as a [string](../../reference/query-language/types/strings.md)) holding the closing price.
- `show` tells Deephaven to draw the plot in the console.

```groovy test-set=8 order=cryptoTrades,btcBin,tOHLC,plotOHLC default=plotOHLC
import static io.deephaven.csv.CsvTools.readCsv
import static io.deephaven.api.agg.Aggregation.AggAvg

cryptoTrades = readCsv("https://media.githubusercontent.com/media/deephaven/examples/main/CryptoCurrencyHistory/CSV/CryptoTrades_20210922.csv")

agg_list = [
    AggLast("CloseTime = Timestamp"),\
    AggFirst("OpenTime = Timestamp"),\
    AggMax("High = Price"),\
    AggMin("Low = Price"),
]

btcBin = cryptoTrades.where("Instrument=`BTC/USD`").update("TimestampBin = lowerBin(Timestamp, MINUTE)")
tOHLC = btcBin.aggBy(agg_list, "TimestampBin").\
 join(btcBin,"CloseTime = Timestamp","Close = Price").\
 join(btcBin,"OpenTime = Timestamp","Open = Price")


plotOHLC = ohlcPlot("BTC", tOHLC, "TimestampBin", "Open", "High", "Low", "Close")\
   .chartTitle("BTC OHLC - Aug 22 2021")\
   .show()
```

This query plots the OHLC chart as follows:

- `plotOHLC` is the name of the variable that will hold the chart.
- `ohlcPlot` is the method.
- `"BTC"` is the name of the series to be used in the chart.
- `tOHLC` is the table from which our data is being pulled.
- `TimestampBin` is the name of the column to be used for the X axis.
- `"Open"`, `"High"`, `"Low"`, and `"Close"`, are the names of the columns containing the four respective data points to be plotted on the Y axis.
- `lineStyle()` and `chartTitle()` provide component formatting to the table. `2` refers to line width.

### Shared axes

Just like XY series plots, the Open, High, Low and Close plot can also be used to present multiple series on the same chart, including the use of multiple X or Y axes. An example of this follows:

```groovy test-set=8 order=btcBin,ethBin,btcOHLC,ethOHLC,plotOHLC default=plotOHLC
btcBin = cryptoTrades.where("Instrument=`BTC/USD`").update("TimestampBin = lowerBin(Timestamp, MINUTE)")
ethBin = cryptoTrades.where("Instrument=`ETH/USD`").update("TimestampBin = lowerBin(Timestamp, MINUTE)")

agg_list = [
    AggLast("CloseTime = Timestamp"),\
    AggFirst("OpenTime = Timestamp"),\
    AggMax("High = Price"),\
    AggMin("Low = Price"),
]

btcOHLC = btcBin.aggBy(agg_list, "TimestampBin").\
 join(btcBin,"CloseTime = Timestamp","Close = Price").\
 join(btcBin,"OpenTime = Timestamp","Open = Price")

ethOHLC = ethBin.aggBy(agg_list, "TimestampBin").\
 join(ethBin,"CloseTime = Timestamp","Close = Price").\
 join(ethBin,"OpenTime = Timestamp","Open = Price")

plotOHLC = ohlcPlot("BTC", btcOHLC, "TimestampBin", "Open", "High", "Low", "Close")\
   .chartTitle("BTC and ETC OHLC - Aug 22 2021")\
   .twinX()\
   .ohlcPlot("ETH", ethOHLC, "TimestampBin", "Open", "High", "Low", "Close")\
   .show()
```

This query plots the OHLC chart as follows:

- `plotOHLC` is the name of the variable that will hold the chart.
  - `ohlcPlot` plots the first series.
  - `"BTC"` is the name of the first series to be used in the chart.
  - `btcOHLC` is the table from which the data is being pulled.
  - ``where("Instrument=`BTC/USD`")`` filters the table to only the AAPL Ticker.
  - `TimestampBin` is the name of the column to be used for the X axis.
  - `"Open"`, `"High"`, "`Low"`, and `"Close"`, are the names of the columns containing the four respective data points to be plotted on the Y axis.
- `twinX` is used to show different Y axes.
- `ohlcPlot` plots the second series.
  - `"ETH"` is the name of the second series to be used in the chart.
  - `ethOHLC` is the table from which the data is being pulled.
  - ``where("Instrument=`ETH/USD`")`` filters the table to only the MSFT Ticker.
  - `TimestampBin` is the name of the column to be used for the X axis.
  - `"Open"`, `"High"`, `"Low"`, and `"Close"`, are the names of the columns containing the four respective data points to be plotted on the Y axis.
- `chartTitle()` provides the title for the chart.

In this plot, the opening, high, low and closing price of BTC and ETH are plotted. The `twinX()` method is used to show the value scale for BTC on the left Y axis and the value scale for ETH on the right Y axis.

## Pie

Use the [`piePlot`](../../reference/plot/piePlot.md) method to create pie charts, which show data as sections of a circle to represent the relative proportion for each of the categories that make up the entire dataset being plotted.

When data is sourced from a table, the following syntax can be used to create a pie plot:

`.piePlot(seriesName, source, categoryCol, valueCol).show()`

- `piePlot` is the method used to create a pie plot.
- `seriesName` is the name (as a [string](../../reference/query-language/types/strings.md)) you want to use to identify the series on the plot itself.
- `source` is the table that holds the data you want to plot.
- `categoryCol` is the name of the column (as a [string](../../reference/query-language/types/strings.md)) to be used for the categories.
- `valueCol` is the name of the column (as a [string](../../reference/query-language/types/strings.md)) to be used for the values.
- `show` tells Deephaven to draw the plot in the console.

```groovy test-set=9 order=totalInsurance,regionInsurance,pieChart default=pieChart
import static io.deephaven.csv.CsvTools.readCsv

totalInsurance = readCsv("https://media.githubusercontent.com/media/deephaven/examples/main/Insurance/csv/insurance.csv")

regionInsurance = totalInsurance.view("region", "expenses").sumBy("region")

pieChart = piePlot("Expense per region", regionInsurance, "region", "expenses")\
     .chartTitle("Expenses per region")\
     .show()
```

## Dynamic plots

Plots configured with a [`SelectableDataSetOneClick`](../../reference/query-language/types/SelectableDataSetOneClick.md) can be paired with Deephaven's [Input Filter](../user-interface/filters.md#input-filters), [Linker feature](../user-interface/filters.md#linker), or drop-down filters. As the input changes, the paired plot will dynamically update to reflect the filtered dataset.

### `oneClick`

To create a dynamically updating plot - a plot that updates with "one click" in the console -, the data source must be a [`SelectableDataSetOneClick`](../../reference/query-language/types/SelectableDataSetOneClick.md). This is easily accomplished with the use of the [`oneClick`](../../reference/plot/oneClick.md) method:

```syntax
oneClick(pTable)
oneClick(pTable, requireAllFiltersToDisplay)
oneClick(t, requireAllFiltersToDisplay, byColumns)
oneClick(t, byColumns...)
```

- `pTable` is a partitioned table containing the data.
- `t` is the table containing the data.
- `byColumns` is a list of strings, where each string is the name of a column to be made available for input filtering.
- `requireAllFiltersToDisplay`, when set to `true`, will display a prompt explaining that filter controls must be added to the resulting plot. By default (`false`), this message will not be displayed.

```groovy skip-test
import static io.deephaven.csv.CsvTools.readCsv

source = readCsv("https://media.githubusercontent.com/media/deephaven/examples/main/CryptoCurrencyHistory/CSV/CryptoTrades_20210922.csv")
oc = oneClick(source, "Instrument")
plot = plot("Plot", oc, "Timestamp", "Price").show()
```

![The above table and plot displayed in the IDE](../../assets/how-to/oc-result-unfiltered.png)

### Input Filters

To pair your dynamic plot with an [Input Filter](../user-interface/filters.md#input-filters), navigate to **Input Filter** in the **Controls** menu in the upper right-hand corner of the UI, or select **Add Input Filters** in the console.

![A user selects the **Input Filter** option from the **Controls** menu](../../assets/how-to/ui-input-filter-gr.gif)

> [!NOTE]
> The Input Filter feature filters _every_ open table and dynamic plot in the console.

### Linker Tool

Alternatively, you can use the [Linker](../user-interface/filters.md#linker) tool to pair the dynamic plot with a table by clicking the **Open Linker Tool** button in the console or choosing the **Linker** option from the **Controls** menu.

![The Linker option highlighted in the **Controls** menu](../../assets/how-to/linker-button.png)

![A user pairs a dynamic plot with a table using the Linker tool](../../assets/how-to/linker-tool.gif)

Connect the input column button in the dynamic plot to the matching column in the trigger table. Double-clicking on any row in the trigger table will filter the target (the dynamic plot) to match the input column's value in that row.

## Related documentation

- [How to use the Chart Builder](../user-interface/chart-builder.md)
- [Arrays](../../reference/query-language/types/arrays.md)
- [`catPlot`](../../reference/plot/catPlot.md)
- [`catHistPlot`](../../reference/plot/catHistPlot.md)
- [`histPlot`](../../reference/plot/histPlot.md)
- [`ohlcPlot`](../../reference/plot/ohlcPlot.md)
- [`oneClick`](../../reference/plot/oneClick.md)
- [`piePlot`](../../reference/plot/piePlot.md)
- [`plot`](../../reference/plot/plot.md)
- [`plotStyle`](../../reference/plot/plotStyle.md)
- [`SelectableDataSetOneClick`](../../reference/query-language/types/SelectableDataSetOneClick.md)
