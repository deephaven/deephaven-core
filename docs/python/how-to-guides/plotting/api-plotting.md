---
title: Built-in plotting API
---

Deephaven's built-in plotting API offers methods for creating real-time figures, plots, and charts from static and ticking tables. This guide provides an overview of its capabilities.

> [!NOTE]
> The built-in plotting API is no longer under active development, and there are no plans for new patches, features, or fixes. Deephaven recommends [Deephaven Express](/core/plotly/docs/) for most plotting tasks.

This guide covers the following plot types:

- [XY series plots](#xy-series)
- [Category plots](#category)
- [Category histograms](#category-histogram)
- [Histograms](#histogram)
- [OHLC plots](#ohlc)
- [Pie charts](#pie)
- [Treemaps](#treemaps)

It also shows how to [combine plots in subplots](#subplots) and [make plots that users filter from the UI](#dynamic-plots).

Every plot starts from a [`Figure`](/core/pydoc/code/deephaven.plot.figure.html#deephaven.plot.figure.Figure) object. You create one with the [`Figure`](/core/pydoc/code/deephaven.plot.figure.html#deephaven.plot.figure.Figure) constructor, chain plotting methods onto it, and finish with [`show`](/core/pydoc/code/deephaven.plot.figure.html#deephaven.plot.figure.Figure.show).

## XY series

XY series plots generally show values over a continuum, such as time. They can take the form of a line, a bar, an area, or a collection of points. The X axis shows the domain, while the Y axis shows the related values at specific points in the range.

You can create an XY series plot using data from Deephaven tables with the following syntax:

```python syntax
Figure().plot_xy(series_name="series_name", t=t, x="XColumn", y="YColumn").show()
```

- [`plot_xy`](../../reference/plot/plot.md) is the method used to create an XY series plot.
- `series_name` is the name (as a [string](../../reference/query-language/types/strings.md)) you want to use to identify the series on the plot itself.
- `t` is the table that holds the data you want to plot.
- `x` is the name of the column that holds the X values.
- `y` is the name of the column that holds the Y values.
- [`show`](/core/pydoc/code/deephaven.plot.figure.html#deephaven.plot.figure.Figure.show) returns a displayable version of the figure. Assign the result to a variable to see the plot in the console.

The example below creates an XY series plot of distance traveled over time during a metric-century bike ride. The [`where`](../../reference/table-operations/filter/where.md) filter drops the rows where the rider was stopped.

```python test-set=1 order=source,plot_single default=plot_single
from deephaven import read_csv
from deephaven.plot.figure import Figure

# source the data
source = read_csv(
    "https://media.githubusercontent.com/media/deephaven/examples/main/MetricCentury/csv/metriccentury.csv"
)

# plot the data
plot_single = (
    Figure()
    .plot_xy(
        series_name="Distance",
        t=source.where(filters=["SpeedKPH > 0"]),
        x="Time",
        y="DistanceMeters",
    )
    .show()
)
```

### Shared axes

You can compare multiple series over the same period of time by creating an XY series plot with shared axes. In the following example, two series create two line graphs on the same plot.

```python test-set=1 order=plot_shared_axis
# plot the data
plot_shared_axis = (
    Figure()
    .plot_xy(series_name="Altitude", t=source, x="Time", y="AltitudeMeters")
    .plot_xy(series_name="Speed", t=source, x="Time", y="SpeedKPH")
    .show()
)
```

To add more series to the plot, chain more [`plot_xy`](../../reference/plot/plot.md) calls onto the figure.

> [!TIP]
> To hide a series, click its name at the right of the plot. Click the name again to restore it.

### Plot multiple series in a loop

It's common in data analysis to have multiple series that share an X axis. In that case, a `for` loop can add each series to the figure. The following example plots three different series on the same plot in a `for` loop.

```python test-set=2 order=loop_source,plot_loop default=plot_loop
from deephaven.plot.figure import Figure
from deephaven import empty_table

loop_source = empty_table(10).update(
    ["X = i", "Y1 = randomInt(1, 10)", "Y2 = randomInt(3, 11)", "Y3 = randomInt(5, 15)"]
)

fig = Figure()

for idx in range(1, 4):
    fig = fig.plot_xy(series_name=f"Y{idx}", t=loop_source, x="X", y=f"Y{idx}")

plot_loop = fig.show()
```

### Plot by a grouping column

To plot a separate series for each key, pass the grouping columns to the `by` parameter of [`plot_xy`](../../reference/plot/plot.md). Deephaven plots one XY series for each unique value, or combination of values, in the `by` columns.

```python test-set=1 order=plot_multi
from deephaven.plot.figure import Figure
from deephaven import empty_table

multi_source = empty_table(20).update(
    ["Letter = (i % 2 == 0) ? `A` : `B`", "X = 0.1 * i", "Y = randomDouble(0.0, 5.0)"]
)

plot_multi = (
    Figure()
    .plot_xy(series_name="Random numbers", t=multi_source, x="X", y="Y", by=["Letter"])
    .show()
)
```

### Multiple X or Y axes

When series in one plot have very different value ranges, a shared Y axis has to cover all of them, so the smaller series flatten and their changes become hard to see.

To fix this, add a second Y axis with the [`x_twin`](/core/pydoc/code/deephaven.plot.figure.html#deephaven.plot.figure.Figure.x_twin) method. [`x_twin`](/core/pydoc/code/deephaven.plot.figure.html#deephaven.plot.figure.Figure.x_twin) enables you to use one Y axis for some of the series being plotted and a second Y axis for the others, while sharing the same X axis:

```python syntax
plot_name = Figure().plot_xy(...).x_twin().plot_xy(...).show()
```

- Series plotted _before_ [`x_twin`](/core/pydoc/code/deephaven.plot.figure.html#deephaven.plot.figure.Figure.x_twin) share the left Y axis.
- Series plotted _after_ [`x_twin`](/core/pydoc/code/deephaven.plot.figure.html#deephaven.plot.figure.Figure.x_twin) share the right Y axis.
- All series share the same X axis.

In the following example, distance reaches about 100,000 meters while speed stays below 70 KPH, so each series gets its own Y axis:

```python test-set=1 order=plot_shared_twin_x
plot_shared_twin_x = (
    Figure()
    .plot_xy(series_name="Distance", t=source, x="Time", y="DistanceMeters")
    .x_twin()
    .plot_xy(series_name="Speed", t=source, x="Time", y="SpeedKPH")
    .show()
)
```

The left Y axis shows the value range for distance, and the right Y axis shows the value range for speed.

The [`y_twin`](/core/pydoc/code/deephaven.plot.figure.html#deephaven.plot.figure.Figure.y_twin) method enables you to use one X axis for one set of the values being plotted and a second X axis for another, while sharing the same Y axis:

```python syntax
plot_name = Figure().plot_xy(...).y_twin().plot_xy(...).show()
```

- Series plotted _before_ [`y_twin`](/core/pydoc/code/deephaven.plot.figure.html#deephaven.plot.figure.Figure.y_twin) use the lower X axis.
- Series plotted _after_ [`y_twin`](/core/pydoc/code/deephaven.plot.figure.html#deephaven.plot.figure.Figure.y_twin) use the upper X axis.

### Plot colors

It's easy to change the line colors in an XY series plot with the [`line`](/core/pydoc/code/deephaven.plot.figure.html#deephaven.plot.figure.Figure.line) method. This example takes the Distance and Speed plot from [Multiple X or Y axes](#multiple-x-or-y-axes) and draws its lines in red and yellow:

```python test-set=1
plot_shared_twin_x_colors = (
    Figure()
    .plot_xy(series_name="Distance", t=source, x="Time", y="DistanceMeters")
    .line(color="RED")
    .x_twin()
    .plot_xy(series_name="Speed", t=source, x="Time", y="SpeedKPH")
    .line(color="YELLOW")
    .show()
)
```

For the full list of available colors, see [Named colors](../../reference/table-operations/format/named-colors.md).

### Plot styles

The XY series plot in Deephaven defaults to a line plot. Use the `plot_style` parameter of the [`axes`](/core/pydoc/code/deephaven.plot.figure.html#deephaven.plot.figure.Figure.axes) method to format XY series plots as area charts, stacked area charts, bar charts, stacked bar charts, scatter charts, or step charts.

In any of the examples below, you can swap in another [`PlotStyle`](../../reference/plot/plotStyle.md) value, such as `PlotStyle.BAR`, `PlotStyle.STACKED_BAR`, `PlotStyle.LINE`, `PlotStyle.AREA`, `PlotStyle.STACKED_AREA`, `PlotStyle.SCATTER`, or `PlotStyle.STEP`. The `plot_style` parameter also accepts the style name as a string, such as `"AREA"`.

#### XY series as a stacked area plot

The following example stacks cadence on top of heart rate:

```python test-set=1 order=plot_stacked_area
from deephaven.plot import PlotStyle

plot_stacked_area = (
    Figure()
    .axes(plot_style=PlotStyle.STACKED_AREA)
    .plot_xy(series_name="Heart_rate", t=source, x="Time", y="HeartRate")
    .plot_xy(series_name="Cadence", t=source, x="Time", y="Cadence")
    .show()
)
```

#### XY series as a scatter plot

In the example below, both series use the [`PlotStyle.SCATTER`](../../reference/plot/plotStyle.md) plot style. [`x_twin`](/core/pydoc/code/deephaven.plot.figure.html#deephaven.plot.figure.Figure.x_twin) keeps the shared X axis and gives each series its own Y axis.

```python test-set=1 order=plot_xy_scatter
from deephaven.plot.figure import Figure
from deephaven.plot import PlotStyle

plot_xy_scatter = (
    Figure()
    .plot_xy(series_name="Speed", t=source, x="Time", y="SpeedKPH")
    .axes(plot_style=PlotStyle.SCATTER)
    .x_twin()
    .plot_xy(series_name="Distance", t=source, x="Time", y="DistanceMeters")
    .axes(plot_style=PlotStyle.SCATTER)
    .show()
)
```

#### XY series with markers on selected points

In the example below, a line plot of speed gets a marker at every point where the rider went faster than 40 KPH.

- The [`twin`](/core/pydoc/code/deephaven.plot.figure.html#deephaven.plot.figure.Figure.twin) method adds a second set of axes that shares both the X and Y axes.
- [`axes`](/core/pydoc/code/deephaven.plot.figure.html#deephaven.plot.figure.Figure.axes) then gives those axes the [`PlotStyle.SCATTER`](../../reference/plot/plotStyle.md) style, so the series plotted on them appears as markers over the line.

```python test-set=1 order=plot_xy_markers
fast_points = source.where(filters=["SpeedKPH > 40"])

plot_xy_markers = (
    Figure()
    .plot_xy(series_name="Speed", t=source, x="Time", y="SpeedKPH")
    .twin()
    .axes(plot_style=PlotStyle.SCATTER)
    .plot_xy(series_name="Over 40 KPH", t=fast_points, x="Time", y="SpeedKPH")
    .show()
)
```

#### XY series as a step plot

In the example below, [`axes(plot_style=PlotStyle.STEP)`](/core/pydoc/code/deephaven.plot.figure.html#deephaven.plot.figure.Figure.axes) draws a step plot. Then [`line(style=LineStyle(...))`](/core/pydoc/code/deephaven.plot.figure.html#deephaven.plot.figure.Figure.line) sets the line width and end style through a [`LineStyle`](/core/pydoc/code/deephaven.plot.linestyle.html#deephaven.plot.linestyle.LineStyle) object.

```python test-set=1 order=plot_step
from deephaven.plot import LineEndStyle, LineStyle

plot_step = (
    Figure()
    .axes(plot_style=PlotStyle.STEP)
    .plot_xy(series_name="HeartRate", t=source, x="Time", y="HeartRate")
    .line(style=LineStyle(width=3.0, end_style=LineEndStyle.SQUARE))
    .show()
)
```

## Category

Use the [`plot_cat`](../../reference/plot/catPlot.md) method to create category plots, which display data values from different discrete categories.

To plot data from a Deephaven table, use the following syntax:

```python syntax
Figure().plot_cat(series_name="series_name", t=t, category="Category", y="Y").show()
```

- [`plot_cat`](../../reference/plot/catPlot.md) is the method used to create a category plot.
- `series_name` is the name (as a [string](../../reference/query-language/types/strings.md)) you want to use to identify the series on the plot itself.
- `t` is the table that holds the data you want to plot.
- `category` is the name of the column that holds the category values.
- `y` is the name of the column that holds the Y values.
- `y_low` and `y_high` (optional) are the names of columns that hold lower and upper Y error-bar values. Passing both creates a category error-bar plot.
- `by` (optional) is a list of one or more columns that hold grouping data. Deephaven plots a separate series for each unique value, or combination of values, in the `by` columns.
- [`show`](/core/pydoc/code/deephaven.plot.figure.html#deephaven.plot.figure.Figure.show) returns a displayable version of the figure. Assign the result to a variable to see the plot in the console.

```python order=source,plot_cat default=plot_cat
from deephaven.column import int_col, string_col
from deephaven.plot.figure import Figure
from deephaven import new_table

source = new_table(
    [string_col("Categories", ["A", "B", "C"]), int_col("Values", [1, 3, 5])]
)

plot_cat = (
    Figure()
    .plot_cat(series_name="Categories", t=source, category="Categories", y="Values")
    .show()
)
```

### Shared axes

You can compare multiple series across the same categories by creating a category plot with shared axes. The following example plots two tables with the same `Categories` column as bar graphs on one chart:

```python test-set=3 order=cat_shared_axes,source_one,source_two
from deephaven.plot.figure import Figure
from deephaven.column import int_col, string_col
from deephaven import new_table

source_one = new_table(
    [string_col("Categories", ["A", "B", "C"]), int_col("Values", [1, 3, 5])]
)
source_two = new_table(
    [string_col("Categories", ["A", "B", "C"]), int_col("Values", [2, 4, 6])]
)

cat_shared_axes = (
    Figure()
    .plot_cat(series_name="source_one", t=source_one, category="Categories", y="Values")
    .plot_cat(series_name="source_two", t=source_two, category="Categories", y="Values")
    .show()
)
```

To add more series to the chart, chain more [`plot_cat`](../../reference/plot/catPlot.md) calls onto the figure.

### Plot styles

By default, category plots draw values as vertical bars. To use another style, pass a [`PlotStyle`](../../reference/plot/plotStyle.md) value, such as `PlotStyle.STACKED_BAR`, `PlotStyle.LINE`, or `PlotStyle.AREA`, to the `plot_style` parameter of the [`axes`](/core/pydoc/code/deephaven.plot.figure.html#deephaven.plot.figure.Figure.axes) method.

#### Category plot with stacked bars

The following example draws the `source_one` and `source_two` series from the [category shared axes example](#shared-axes-1) as stacked bars:

```python test-set=3 order=cat_stacked_bar
from deephaven.plot import PlotStyle

cat_stacked_bar = (
    Figure()
    .axes(plot_style=PlotStyle.STACKED_BAR)
    .plot_cat(series_name="source_one", t=source_one, category="Categories", y="Values")
    .plot_cat(series_name="source_two", t=source_two, category="Categories", y="Values")
    .show()
)
```

## Category histogram

Use the [`plot_cat_hist`](../../reference/plot/catHistPlot.md) method to create category histograms, which show how often each discrete value (category) occurs.

To plot data from a Deephaven table, use the following syntax:

```python syntax
Figure().plot_cat_hist(series_name="series_name", t=t, category="Category").show()
```

- [`plot_cat_hist`](../../reference/plot/catHistPlot.md) is the method used to create a category histogram.
- `series_name` is the name (as a [string](../../reference/query-language/types/strings.md)) you want to use to identify the series on the chart itself.
- `t` is the table that holds the data you want to plot.
- `category` is the name of the column (as a [string](../../reference/query-language/types/strings.md)) containing the discrete values.
- [`show`](/core/pydoc/code/deephaven.plot.figure.html#deephaven.plot.figure.Figure.show) returns a displayable version of the figure. Assign the result to a variable to see the plot in the console.

```python test-set=4 order=source,plot_cat_hist default=plot_cat_hist
from deephaven import new_table
from deephaven.column import int_col
from deephaven.plot.figure import Figure

source = new_table([int_col("Keys", [3, 2, 2, 1, 1, 1])])


plot_cat_hist = (
    Figure()
    .plot_cat_hist(series_name="Category Histogram", t=source, category="Keys")
    .show()
)
```

To plot data from a Python list instead of a table, use the following syntax:

```python syntax
Figure().plot_cat_hist(series_name="series_name", category=values).show()
```

- `category` is the list of discrete values to count.

The following example counts values from a NumPy array, converted to a list with `list(values)`:

```python test-set=5 order=plot_cat_hist2
from deephaven.plot.figure import Figure
import numpy as np

values = np.array([3, 2, 2, 1, 1, 1])

plot_cat_hist2 = (
    Figure()
    .plot_cat_hist(series_name="Values", category=list(values))
    .chart_title(title="Count of Values")
    .show()
)
```

[`chart_title`](/core/pydoc/code/deephaven.plot.figure.html#deephaven.plot.figure.Figure.chart_title) sets the title of the chart the series is drawn on.

## Histogram

Use the [`plot_xy_hist`](../../reference/plot/histPlot.md) method to create histograms.

A histogram shows how frequently different data values occur. It divides the data into intervals (bins), counts the values in each bin, and draws each count as a vertical bar. Unlike bar charts (category plots), bars in histograms do not have spaces between them unless there is a gap in the data.

To plot data from a Deephaven table, use the following syntax:

```python syntax
Figure().plot_xy_hist(series_name="series_name", t=t, x="XColumn", nbins=10).show()
```

- [`plot_xy_hist`](../../reference/plot/histPlot.md) is the method used to create a histogram.
- `series_name` is the name (as a [string](../../reference/query-language/types/strings.md)) you want to use to identify the series on the chart itself.
- `t` is the table that holds the data you want to plot.
- `x` is the name of the column (as a [string](../../reference/query-language/types/strings.md)) that holds the X values.
- `nbins` is the integer number of intervals to use in the chart.
- [`show`](/core/pydoc/code/deephaven.plot.figure.html#deephaven.plot.figure.Figure.show) returns a displayable version of the figure. Assign the result to a variable to see the plot in the console.

```python test-set=5 order=plot_hist
from deephaven.plot.figure import Figure
from deephaven import new_table
from deephaven.column import int_col

source = new_table([int_col("Values", [1, 2, 2, 3, 3, 3, 4, 4, 5])])

plot_hist = (
    Figure()
    .plot_xy_hist(series_name="Histogram Values", t=source, x="Values", nbins=5)
    .chart_title(title="Histogram of Values")
    .show()
)
```

The [`plot_xy_hist`](../../reference/plot/histPlot.md) method assumes you want to plot the entire range of values in the dataset. You can also set the minimum and maximum values of the range with `xmin` and `xmax`, respectively:

```python syntax
Figure().plot_xy_hist(
    series_name="series_name", t=t, x="XColumn", xmin=2.0, xmax=4.0, nbins=10
).show()
```

- `xmin` is a floating-point minimum value in the `x` column you want to include in the plot.
- `xmax` is a floating-point maximum value in the `x` column you want to include in the plot.

```python test-set=5 order=plot_hist_min_max_values
plot_hist_min_max_values = (
    Figure()
    .plot_xy_hist(
        series_name="Histogram Values",
        t=source,
        x="Values",
        xmin=2.0,
        xmax=4.0,
        nbins=5,
    )
    .chart_title(title="Histogram of Values")
    .show()
)
```

To plot data from a Python list instead of a table, use the following syntax:

```python syntax
Figure().plot_xy_hist(series_name="series_name", x=values, nbins=10).show()
```

- `x` is the list of values to bin.

The following example plots values from a NumPy array, converted to a list with `list(values)`:

```python test-set=6 order=plot_hist_list
from deephaven.plot.figure import Figure
from numpy import array

values = array([1, 2, 2, 3, 3, 3, 4, 4, 5])

plot_hist_list = (
    Figure()
    .plot_xy_hist(series_name="Histogram Values", x=list(values), nbins=5)
    .chart_title(title="Histogram of Values")
    .show()
)
```

## OHLC

Use the [`plot_ohlc`](../../reference/plot/ohlcPlot.md) method to create Open, High, Low, and Close (OHLC) plots. These typically show four prices of a security or commodity per time slice: the open and close of the time slice, and the highest and lowest values reached during the time slice.

This plotting method requires a dataset that includes one column containing the values for the X axis (time), and one column for each of the corresponding four values (open, high, low, close).

To plot data from a Deephaven table, use the following syntax:

```python syntax
Figure().plot_ohlc(
    series_name="series_name",
    t=t,
    x="X",
    open="Open",
    high="High",
    low="Low",
    close="Close",
).show()
```

- [`plot_ohlc`](../../reference/plot/ohlcPlot.md) is the method used to create an OHLC chart.
- `series_name` is the name (as a [string](../../reference/query-language/types/strings.md)) you want to use to identify the series on the chart itself.
- `t` is the table that holds the data you want to plot.
- `x` is the name of the column to use for the X axis.
- `open` is the name of the column holding the opening price.
- `high` is the name of the column holding the highest price.
- `low` is the name of the column holding the lowest price.
- `close` is the name of the column holding the closing price.
- `by` (optional) is a list of one or more columns that hold grouping data. Deephaven plots a separate OHLC series for each unique value, or combination of values, in the `by` columns.
- [`show`](/core/pydoc/code/deephaven.plot.figure.html#deephaven.plot.figure.Figure.show) returns a displayable version of the figure. Assign the result to a variable to see the plot in the console.

The following example bins BTC trades into one-minute intervals and uses an [aggregation](../../reference/table-operations/group-and-aggregate/aggBy.md) to compute each interval's open, high, low, and close prices. The trades in the file are not in time order, so the example [sorts](../../reference/table-operations/sort/sort.md) them by `Timestamp` first. That way, [`agg.first`](../../reference/table-operations/group-and-aggregate/AggFirst.md) and [`agg.last`](../../reference/table-operations/group-and-aggregate/AggLast.md) return each interval's earliest and latest trade.

```python test-set=7 order=ohlc_plot,crypto_trades,btc_bin,t_ohlc
from deephaven.plot.figure import Figure
from deephaven import agg
from deephaven import read_csv


crypto_trades = read_csv(
    "https://media.githubusercontent.com/media/deephaven/examples/main/CryptoCurrencyHistory/CSV/CryptoTrades_20210922.csv"
)


agg_list = [
    agg.first(cols=["Open = Price"]),
    agg.max_(cols=["High = Price"]),
    agg.min_(cols=["Low = Price"]),
    agg.last(cols=["Close = Price"]),
]

btc_bin = (
    crypto_trades.where(filters=["Instrument == `BTC/USD`"])
    .update(formulas=["TimestampBin = lowerBin(Timestamp, MINUTE)"])
    .sort(order_by=["Timestamp"])
)

t_ohlc = btc_bin.agg_by(agg_list, by=["TimestampBin"])

ohlc_plot = (
    Figure()
    .figure_title("BTC OHLC - Sep 22 2021")
    .plot_ohlc(
        series_name="BTC",
        t=t_ohlc,
        x="TimestampBin",
        open="Open",
        high="High",
        low="Low",
        close="Close",
    )
    .show()
)
```

[`figure_title`](/core/pydoc/code/deephaven.plot.figure.html#deephaven.plot.figure.Figure.figure_title) sets the title of the whole figure, which can hold several charts (see [Subplots](#subplots)).

### Plot by a grouping column

To plot a separate OHLC series for each key, pass the grouping columns to the `by` parameter of [`plot_ohlc`](../../reference/plot/ohlcPlot.md). The following example bins BTC and ETH trades together, computes OHLC values for each instrument and interval, and plots one series per instrument:

```python test-set=7 order=ohlc_by_plot,t_ohlc_by
# Bin the BTC and ETH trades into one-minute intervals
two_coins_bin = (
    crypto_trades.where(filters=["Instrument in `BTC/USD`, `ETH/USD`"])
    .update(formulas=["TimestampBin = lowerBin(Timestamp, MINUTE)"])
    .sort(order_by=["Timestamp"])
)

# Compute OHLC values per instrument and interval
t_ohlc_by = two_coins_bin.agg_by(agg_list, by=["Instrument", "TimestampBin"])

# Plot one OHLC series for each instrument
ohlc_by_plot = (
    Figure()
    .plot_ohlc(
        series_name="Crypto",
        t=t_ohlc_by,
        x="TimestampBin",
        open="Open",
        high="High",
        low="Low",
        close="Close",
        by=["Instrument"],
    )
    .show()
)
```

All the series share one Y axis. ETH trades at about one-fourteenth of BTC's price, so its series looks flat next to BTC's. To give each series its own Y axis, plot them separately, as the next section shows.

### Multiple series with separate Y axes

Like XY series plots, OHLC plots can show multiple series on one chart. The following example repeats the BTC binning from the [OHLC example](#ohlc), adds a second series for ETH, and uses [`x_twin`](/core/pydoc/code/deephaven.plot.figure.html#deephaven.plot.figure.Figure.x_twin) to give ETH its own Y axis:

```python test-set=7 order=btc_bin,eth_bin,btc_ohlc,eth_ohlc,ohlc_plot default=ohlc_plot
from deephaven import agg
from deephaven.plot.figure import Figure

btc_bin = (
    crypto_trades.where(filters=["Instrument == `BTC/USD`"])
    .update(formulas=["TimestampBin = lowerBin(Timestamp, MINUTE)"])
    .sort(order_by=["Timestamp"])
)

eth_bin = (
    crypto_trades.where(filters=["Instrument == `ETH/USD`"])
    .update(formulas=["TimestampBin = lowerBin(Timestamp, MINUTE)"])
    .sort(order_by=["Timestamp"])
)

agg_list = [
    agg.first(cols=["Open = Price"]),
    agg.max_(cols=["High = Price"]),
    agg.min_(cols=["Low = Price"]),
    agg.last(cols=["Close = Price"]),
]

btc_ohlc = btc_bin.agg_by(agg_list, by=["TimestampBin"])

eth_ohlc = eth_bin.agg_by(agg_list, by=["TimestampBin"])

ohlc_plot = (
    Figure()
    .figure_title("BTC and ETH OHLC - Sep 22 2021")
    .plot_ohlc(
        series_name="BTC",
        t=btc_ohlc,
        x="TimestampBin",
        open="Open",
        high="High",
        low="Low",
        close="Close",
    )
    .x_twin()
    .plot_ohlc(
        series_name="ETH",
        t=eth_ohlc,
        x="TimestampBin",
        open="Open",
        high="High",
        low="Low",
        close="Close",
    )
    .show()
)
```

The second [`plot_ohlc`](../../reference/plot/ohlcPlot.md) call adds an ETH series built the same way as the BTC series. The [`x_twin`](/core/pydoc/code/deephaven.plot.figure.html#deephaven.plot.figure.Figure.x_twin) call between them gives ETH its own Y axis, so the left Y axis shows the BTC price scale and the right Y axis shows the ETH price scale.

## Pie

Use the [`plot_pie`](../../reference/plot/piePlot.md) method to create pie charts, which show each category's share of the total as a slice of a circle.

To create a pie chart from a table, use the following syntax:

```python syntax
Figure().plot_pie(series_name="series_name", t=t, category="Category", y="Y").show()
```

- [`plot_pie`](../../reference/plot/piePlot.md) is the method used to create a pie chart.
- `series_name` is the name (as a [string](../../reference/query-language/types/strings.md)) you want to use to identify the series on the plot itself.
- `t` is the table that holds the data you want to plot.
- `category` is the name of the column that holds the categories to place in the pie chart.
- `y` is the name of the column that holds the value for each category.
- [`show`](/core/pydoc/code/deephaven.plot.figure.html#deephaven.plot.figure.Figure.show) returns a displayable version of the figure. Assign the result to a variable to see the plot in the console.

```python test-set=8 order=plot_pie,insurance,insurance_by_region
from deephaven.plot.figure import Figure
from deephaven import read_csv

insurance = read_csv(
    "https://media.githubusercontent.com/media/deephaven/examples/main/Insurance/csv/insurance.csv"
)
insurance_by_region = insurance.view(formulas=["region", "expenses"]).sum_by(["region"])

plot_pie = (
    Figure()
    .plot_pie(
        series_name="Insurance charges by region",
        t=insurance_by_region,
        category="region",
        y="expenses",
    )
    .show()
)
```

## Treemaps

Use the [`plot_treemap`](../../reference/plot/treemapPlot.md) method to create treemap charts, which show hierarchical data as nested rectangles, where each item's area is proportional to its value.

### From a formatted table

Your table may already have the structure [`plot_treemap`](../../reference/plot/treemapPlot.md) requires. It needs at least two columns:

- An ID column, which identifies each item.
- A parent column, which names the item each one sits under.

Following the parents up from any item must end at one top-level value, called the root. The root doesn't need a row of its own. In the examples below, `root` appears only in the parent column. When your table has this structure, use the following syntax:

```python syntax
Figure().plot_treemap(
    series_name="series_name", t=t, id="IdColumn", parent="ParentColumn"
).show()
```

- `series_name` is the name (as a [string](../../reference/query-language/types/strings.md)) you want to use to identify the series on the plot itself.
- `t` is the table that holds the data you want to plot.
- `id` is the name of the column that holds the item IDs to use in structuring the treemap chart.
- `parent` is the name of the column that holds each item's parent ID.
- [`show`](/core/pydoc/code/deephaven.plot.figure.html#deephaven.plot.figure.Figure.show) returns a displayable version of the figure. Assign the result to a variable to see the plot in the console.

```python test-set=13 order=s_and_p,source
from deephaven import read_csv
from deephaven.plot.figure import Figure

# source the data
source = read_csv(
    "https://media.githubusercontent.com/media/deephaven/examples/main/StandardAndPoors500Financials/csv/s_and_p_500_market_cap.csv"
)

# plot the data
s_and_p = (
    Figure()
    .plot_treemap(
        series_name="S&P 500 Companies by Sector", t=source, id="Label", parent="Parent"
    )
    .show()
)
```

You can interact with this plot. Click a group header to expand that group, and click it again to return to the full view. Click an individual cell to expand its contents.

By default, each item takes its color from the theme, and its label is its ID.

The same table also has value, hover text, and color columns. The following example uses them to display more information:

```python test-set=13 order=s_and_p_marketcap
s_and_p_marketcap = (
    Figure()
    .plot_treemap(
        series_name="S&P 500 Market Cap",
        t=source,
        id="Label",
        parent="Parent",
        label="Label",
        value="Value",
        hover_text="HoverText",
        color="Color",
    )
    .show()
)
```

- `label` is the name of the column that holds the display text to use instead of the raw ID.
- `value` is the name of the column that sets how large each item is compared to the others.
- `hover_text` is the name of the column that holds the text to display when hovering over an item.
- `color` is the name of the column that holds the color to use for each item.

The table's `Text` column holds each stock's percent change rather than a name, so the example passes the ID column, `Label`, as `label` too. With a column of display names, pass that column instead.

### From a raw data table

Raw data often lacks the ID and parent columns that [`plot_treemap`](../../reference/plot/treemapPlot.md) needs. The following example builds them with [`view`](../../reference/table-operations/select/view.md), [`select_distinct`](../../reference/table-operations/select/select-distinct.md), and [`merge`](../../reference/table-operations/merge/merge.md):

```python test-set=14 order=s_and_p_treemap,source,financials,sectors,sector_items,stock_items,items
from deephaven import read_csv, merge
from deephaven.plot.figure import Figure

# Source the data
source = read_csv(
    "https://media.githubusercontent.com/media/deephaven/examples/main/StandardAndPoors500Financials/csv/s_and_p_500_companies_financials.csv"
)

# Get a view of just the data we need
financials = source.view(["Symbol", "Name", "Sector", "MarketCap = Market_Cap"])

# Find the distinct sectors defined in the data
sectors = financials.select_distinct(["Sector"]).sort(["Sector"])

# Define `root` as the top parent, use sectors as the first level of children
sector_items = sectors.view(
    [
        "Parent = `root`",
        "Label = Sector",
        "Value = (long)0",
        "Color = `#373438`",
        "HoverText = Sector",
    ]
)


# Define a heat map function that returns a color based on the percent change
def heatmap_daily_perc(perc):
    if perc < -0.02:
        return "#f95d84"
    if perc < -0.01:
        return "#f37e3f"
    if perc > 0.01:
        return "#9edc6f"
    return "#fcd65b"


# Update our data with random percent changes for display purposes
stock_items = financials.update(
    [
        "Perc = randomGaussian(0, 2) / 100.0",
        "PercLabel = String.format(`%.1f%%`, Perc * 100)",
    ]
).view(
    [
        "Parent = Sector",
        "Label = Symbol",
        "Value = (long)MarketCap",
        "Color = (String)heatmap_daily_perc(Perc)",
        "HoverText = Name + `: ` + PercLabel",
    ]
)

# Merge the topmost sectors with the rest of the stock items
items = merge([sector_items, stock_items])

# Plot the data
s_and_p_treemap = (
    Figure()
    .plot_treemap(
        series_name="S&P 500 Daily Change",
        t=items,
        id="Label",
        parent="Parent",
        label="Label",
        value="Value",
        hover_text="HoverText",
        color="Color",
    )
    .show()
)
```

- `financials` is a [view](../../reference/table-operations/select/view.md) of the raw `source` table with only the `Symbol`, `Name`, `Sector`, and `MarketCap` columns.
- `sectors` is a table of the top-level distinct sectors in the raw data.
- `sector_items` is a table of the top-level sectors with label and color information associated with them.
- `heatmap_daily_perc` is a function that returns a color based on the percent change provided.
- `stock_items` is a table with all the stock items and their associated labels.
- `items` [merges](../../reference/table-operations/merge/merge.md) `sector_items` and `stock_items` into the single table that [`plot_treemap`](../../reference/plot/treemapPlot.md) plots.
- `s_and_p_treemap` is a treemap plot created with the [`plot_treemap`](../../reference/plot/treemapPlot.md) method.

## Subplots

A [`Figure`](/core/pydoc/code/deephaven.plot.figure.html#deephaven.plot.figure.Figure) can contain subplots: separate charts arranged in a grid by row and column number.

- A single plot with several series draws them all in one plot area, sharing at least one axis.
- Subplots give each chart its own section of the figure.

Use subplots when the datasets have different units or scales and you don't need to read them against each other. To overlay two series with different scales on one plot instead, see [Multiple X or Y axes](#multiple-x-or-y-axes).

### Define subplots

Set the number of subplots with the `rows` and `cols` parameters when you create a [`Figure`](/core/pydoc/code/deephaven.plot.figure.html#deephaven.plot.figure.Figure).

```python syntax
from deephaven.plot.figure import Figure

f = Figure(rows=n, cols=m)
```

`n` and `m` are the number of rows and columns, respectively.

For a figure with only one plot, specifying `rows` and `cols` is optional. If a figure has more than one subplot, these arguments are required.

### Add subplots to a figure

After defining the figure's grid, add each subplot with a [`new_chart`](/core/pydoc/code/deephaven.plot.figure.html#deephaven.plot.figure.Figure.new_chart) call. Each [`new_chart`](/core/pydoc/code/deephaven.plot.figure.html#deephaven.plot.figure.Figure.new_chart) call creates a chart at a given row and column in the figure, and the plotting methods that follow it draw on that chart. Row and column indices start at 0.

For example, to stack two subplots vertically, give the figure two rows and one column.

```python test-set=11 order=source,f default=f
from deephaven.plot.figure import Figure
from deephaven import empty_table

source = empty_table(100).update(["X = i", "Y = cos(0.1 * i)", "Z = sin(0.1 * i)"])

f = (
    Figure(rows=2, cols=1)
    .new_chart(row=0, col=0)
    .plot_xy(series_name="Y", t=source, x="X", y="Y")
    .new_chart(row=1, col=0)
    .plot_xy(series_name="Z", t=source, x="X", y="Z")
    .show()
)
```

### One row, two columns

Let's instead place the subplots side by side by giving the figure one row and two columns. The series `Y` appears on the left, and `Z` on the right.

```python test-set=11 order=f
f = (
    Figure(rows=1, cols=2)
    .new_chart(row=0, col=0)
    .plot_xy(series_name="Y", t=source, x="X", y="Y")
    .new_chart(row=0, col=1)
    .plot_xy(series_name="Z", t=source, x="X", y="Z")
    .show()
)
```

### Different plot types

A single figure can handle multiple plot types as well as both static and real-time data.

```python test-set=12 ticking-table order=null
from deephaven.column import int_col, string_col
from deephaven import time_table
from deephaven import new_table
from deephaven.plot.figure import Figure

t1 = time_table("PT0.1S").update_view(["X = 0.1 * i", "Y = sin(X)"])

t2 = new_table(
    [string_col("Categories", ["A", "B", "C"]), int_col("Values", [1, 3, 5])]
)

plot_static_and_rt = (
    Figure(rows=2, cols=1)
    .new_chart(row=0, col=0)
    .plot_cat(series_name="Categories", t=t2, category="Categories", y="Values")
    .new_chart(row=1, col=0)
    .plot_xy(series_name="Sine", t=t1, x="X", y="Y")
    .show()
)
```

![The above dynamic plot updating as the source table ticks](../../assets/how-to/subplots-rt.gif)

## Dynamic plots

A dynamic plot redraws to show only the rows that match the value a user picks in a paired filter control: Deephaven's [Input Filter](../user-interface/filters.md#input-filters), [Linker](../user-interface/filters.md#linker), or [Dropdown Filter](../user-interface/filters.md#dropdown-filter). To make a plot dynamic, use a [`SelectableDataSet`](../../reference/query-language/types/SelectableDataSet.md) as its data source.

### `one_click`

The [`one_click`](../../reference/plot/one-click.md) method creates a [`SelectableDataSet`](../../reference/query-language/types/SelectableDataSet.md) from a table:

```python syntax
one_click(t: Table, by: Optional[list[str]] = None, require_all_filters: bool = False) -> SelectableDataSet
```

- `t` is the table containing the data.
- `by` is a list of one or more column names to make available for input filtering. It defaults to `None`, but [`one_click`](../../reference/plot/one-click.md) raises an error unless you pass at least one column.
- `require_all_filters` controls whether the plot waits for filters. When `True`, the plot displays data only after the user sets a filter on every `by` column. When `False` (the default), data displays before the user sets every filter.

The following example sets `require_all_filters=True`, so the plot shows no data until you pick an `Instrument` value. Until then, the plot displays a message with **Add Input Filters** and **Open Linker Tool** buttons:

<!-- skip-test: with requireAllFiltersToDisplay, the figure waits for an input filter, so the snapshotter times out serializing it. The screenshot below shows the expected result. -->

```python skip-test
from deephaven import read_csv
from deephaven.plot.selectable_dataset import one_click
from deephaven.plot.figure import Figure

source = read_csv(
    "https://media.githubusercontent.com/media/deephaven/examples/main/CryptoCurrencyHistory/CSV/CryptoTrades_20210922.csv"
)
oc = one_click(t=source, by=["Instrument"], require_all_filters=True)
plot = Figure().plot_xy(series_name="Plot", t=oc, x="Timestamp", y="Price").show()
```

![The plot waits for a filter on the Instrument column and shows the Add Input Filters and Open Linker Tool buttons](../../assets/how-to/ui/one-click-unfiltered.png)

To create a [`SelectableDataSet`](../../reference/query-language/types/SelectableDataSet.md) from a [partitioned table](../partitioned-tables.md) instead, use [`one_click_partitioned_table`](../../reference/plot/one-click-partitioned-table.md).

### Input filters

To pair your dynamic plot with an [Input Filter](../user-interface/filters.md#input-filters), choose **Input Filter** from the **Controls** menu in the upper right-hand corner of the UI. If the plot is waiting for a filter, you can also click **Add Input Filters** on the plot itself.

![A user selects the Input Filter option from the Controls menu](../../assets/how-to/ui/ui-input-filter-dropdown.gif)

> [!NOTE]
> The [Input Filter](../user-interface/filters.md#input-filters) feature filters every open table in the dashboard that has a column with the same name and type as the filter column. It also filters every dynamic plot whose [`SelectableDataSet`](../../reference/query-language/types/SelectableDataSet.md) filters on such a column. That column is either a column in the `by` parameter of [`one_click`](../../reference/plot/one-click.md), or a key column of the [partitioned table](../partitioned-tables.md) passed to [`one_click_partitioned_table`](../../reference/plot/one-click-partitioned-table.md).

### Linker tool

As an alternative to an [Input Filter](../user-interface/filters.md#input-filters), you can use the [Linker](../user-interface/filters.md#linker) tool to pair a dynamic plot with a table. Choose **Linker** from the **Controls** menu. If the plot is waiting for a filter, you can also click **Open Linker Tool** on the plot.

![The Linker option highlighted in the Controls menu](../../assets/how-to/ui/linker-dropdown.png)

![A user pairs a dynamic plot with a table using the Linker tool](../../assets/how-to/ui/linker-howto.gif)

In the [Linker](../user-interface/filters.md#linker), the table you filter from is the source table, and the dynamic plot is the target. Click the matching column in the source table, then click the column button on the dynamic plot. After that, double-clicking a row in the source table filters the plot to that row's value in the linked column.

### Dropdown filters

You can also pair a dynamic plot with a [Dropdown Filter](../user-interface/filters.md#dropdown-filter), which filters on a value you choose from a list:

1. Choose **Dropdown Filter** from the **Controls** menu.
2. Under **Source Column**, select a column to supply the list of values.
3. Under **Filter Column**, choose the column the plot's [`SelectableDataSet`](../../reference/query-language/types/SelectableDataSet.md) filters on.
4. Click **Save**, then pick a value from the list.

## Related documentation

- [Plotting overview](./overview.md)
- [Deephaven Express](/core/plotly/docs/)
- [How to use the Chart Builder](../user-interface/chart-builder.md)
- [Filter without writing queries](../user-interface/filters.md)
- [Create and use partitioned tables](../partitioned-tables.md)
- [Named colors](../../reference/table-operations/format/named-colors.md)
- [`agg.first`](../../reference/table-operations/group-and-aggregate/AggFirst.md)
- [`agg.last`](../../reference/table-operations/group-and-aggregate/AggLast.md)
- [`agg.max_`](../../reference/table-operations/group-and-aggregate/AggMax.md)
- [`agg.min_`](../../reference/table-operations/group-and-aggregate/AggMin.md)
- [`agg_by`](../../reference/table-operations/group-and-aggregate/aggBy.md)
- [`empty_table`](../../reference/table-operations/create/emptyTable.md)
- [`int_col`](../../reference/table-operations/create/intCol.md)
- [`merge`](../../reference/table-operations/merge/merge.md)
- [`new_table`](../../reference/table-operations/create/newTable.md)
- [`one_click`](../../reference/plot/one-click.md)
- [`one_click_partitioned_table`](../../reference/plot/one-click-partitioned-table.md)
- [`plot_cat`](../../reference/plot/catPlot.md)
- [`plot_cat_hist`](../../reference/plot/catHistPlot.md)
- [`plot_ohlc`](../../reference/plot/ohlcPlot.md)
- [`plot_pie`](../../reference/plot/piePlot.md)
- [`plot_treemap`](../../reference/plot/treemapPlot.md)
- [`plot_xy`](../../reference/plot/plot.md)
- [`plot_xy_hist`](../../reference/plot/histPlot.md)
- [`PlotStyle`](../../reference/plot/plotStyle.md)
- [`read_csv`](../../reference/data-import-export/CSV/readCsv.md)
- [`select_distinct`](../../reference/table-operations/select/select-distinct.md)
- [`SelectableDataSet`](../../reference/query-language/types/SelectableDataSet.md)
- [`sort`](../../reference/table-operations/sort/sort.md)
- [`string_col`](../../reference/table-operations/create/stringCol.md)
- [`sum_by`](../../reference/table-operations/group-and-aggregate/sumBy.md)
- [`time_table`](../../reference/table-operations/create/timeTable.md)
- [`update`](../../reference/table-operations/select/update.md)
- [`update_view`](../../reference/table-operations/select/update-view.md)
- [`view`](../../reference/table-operations/select/view.md)
- [`where`](../../reference/table-operations/filter/where.md)
- [`Figure` Pydoc](/core/pydoc/code/deephaven.plot.figure.html#deephaven.plot.figure.Figure)
- [`axes` Pydoc](/core/pydoc/code/deephaven.plot.figure.html#deephaven.plot.figure.Figure.axes)
- [`chart_title` Pydoc](/core/pydoc/code/deephaven.plot.figure.html#deephaven.plot.figure.Figure.chart_title)
- [`figure_title` Pydoc](/core/pydoc/code/deephaven.plot.figure.html#deephaven.plot.figure.Figure.figure_title)
- [`line` Pydoc](/core/pydoc/code/deephaven.plot.figure.html#deephaven.plot.figure.Figure.line)
- [`LineEndStyle` Pydoc](/core/pydoc/code/deephaven.plot.linestyle.html#deephaven.plot.linestyle.LineEndStyle)
- [`LineStyle` Pydoc](/core/pydoc/code/deephaven.plot.linestyle.html#deephaven.plot.linestyle.LineStyle)
- [`new_chart` Pydoc](/core/pydoc/code/deephaven.plot.figure.html#deephaven.plot.figure.Figure.new_chart)
- [`show` Pydoc](/core/pydoc/code/deephaven.plot.figure.html#deephaven.plot.figure.Figure.show)
- [`twin` Pydoc](/core/pydoc/code/deephaven.plot.figure.html#deephaven.plot.figure.Figure.twin)
- [`x_twin` Pydoc](/core/pydoc/code/deephaven.plot.figure.html#deephaven.plot.figure.Figure.x_twin)
- [`y_twin` Pydoc](/core/pydoc/code/deephaven.plot.figure.html#deephaven.plot.figure.Figure.y_twin)
