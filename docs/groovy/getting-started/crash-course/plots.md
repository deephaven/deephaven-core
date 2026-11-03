---
title: Real-time Plots
---

Whether your data is static or updating in real time, Deephaven supports plotting in Groovy via its built-in [plot package](/core/javadoc/io/deephaven/plot/package-summary.html).

## Basic plots

The built-in plotting library supports many common plot types. To create a simple [line plot](https://en.wikipedia.org/wiki/Line_chart) that ticks in lockstep with the source table, pass a series name, the table, and its x and y columns to [`plot`](../../reference/plot/plot.md), then call [`show`](/core/javadoc/io/deephaven/plot/Figure.html#show()) to display the plot. The series name labels the line in the plot's legend:

```groovy test-set=1 ticking-table order=null
tLine = timeTable("PT0.2s").update("X = 0.05 * ii", "Y1 = X * sin(X)", "Y2 = 5 * X * cos(X)")

plotLine1 = plot("Y1", tLine, "Timestamp", "Y1").show()
```

![A ticking line plot](../../assets/tutorials/crash-course/crash-course-6.gif)

A figure is the container that holds one or more plots. To plot multiple series together in the same figure, start from [`figure`](/core/javadoc/io/deephaven/plot/PlottingConvenience.html#figure()) and chain [`plot`](../../reference/plot/plot.md) calls:

```groovy test-set=1 ticking-table order=null
plotLine2 = figure()
    .plot("Y1", tLine, "Timestamp", "Y1")
    .plot("Y2", tLine, "Timestamp", "Y2")
    .show()
```

![A ticking plot with multiple series](../../assets/tutorials/crash-course/crash-course-7.gif)

## Plots with multiple axes

[`twinX`](/core/javadoc/io/deephaven/plot/Figure.html#twinX()) adds a new set of axes that shares the current x axis, so the plot gets a second y axis. [`twinY`](/core/javadoc/io/deephaven/plot/Figure.html#twinY()) does the same with a shared y axis, which gives a second x axis. The example below uses `twinX` to plot `Y1` and `Y2` against one x axis, each with its own y axis.

```groovy test-set=1 ticking-table order=null
plotTwin = figure()
    .plot("Y1", tLine, "Timestamp", "Y1")
    .twinX()
    .plot("Y2", tLine, "Timestamp", "Y2")
    .show()
```

![A ticking plot with two y axes that share an x axis](../../assets/tutorials/crash-course/crash-course-8.gif)

## Subplots

So far, each figure has held a single plot, even when that plot has several series. A figure can also hold several plots arranged in a grid. For instance, a single figure could contain two plots stacked vertically, two plots side by side, or four plots in a 2x2 grid. To create these subplots, pass the number of rows and columns in the grid to [`figure`](/core/javadoc/io/deephaven/plot/PlottingConvenience.html#figure(int,int)), then place each plot in a cell with [`newChart`](/core/javadoc/io/deephaven/plot/Figure.html#newChart(int,int)), whose row and column indices start at 0.

The example below creates a figure with two subplots side by side.

```groovy test-set=1 ticking-table order=null
plotSub = figure(1, 2)
    .newChart(0, 0)
    .plot("Y1", tLine, "Timestamp", "Y1")
    .newChart(0, 1)
    .plot("Y2", tLine, "Timestamp", "Y2")
    .show()
```

![Two subplots, displayed side by side](../../assets/tutorials/crash-course/crash-course-9.gif)

## More plot types

Far more plots are available in the built-in [plot package](/core/javadoc/io/deephaven/plot/package-summary.html), including [histograms](../../how-to-guides/plotting/api-plotting.md#histogram), [pie charts](../../how-to-guides/plotting/api-plotting.md#pie), and [scatter plots](../../how-to-guides/plotting/api-plotting.md#xy-series-as-a-scatter-plot).

> [!TIP]
> Prefer a no-code approach? Use the [Chart Builder](../../how-to-guides/user-interface/chart-builder.md) to create line, bar, scatter, and pie charts from any table directly in the UI.
