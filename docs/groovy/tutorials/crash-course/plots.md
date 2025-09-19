---
title: Real-time Plots
sidebar_label: Real-time Plots
---

Whether your data is static or updating in real time, Deephaven supports plotting in Groovy via multiple its built-in [plot package](/core/javadoc/io/deephaven/plot/package-summary.html).

## Basic plots

Deephaven's native plotting library supports many common plot types. To create a simple [line plot](https://en.wikipedia.org/wiki/Line_chart) that ticks in lock-step with the source table:

```groovy test-set=1 ticking-table order=null
tLine = timeTable("PT0.2s").update("X = 0.05 * ii", "Y1 = X * sin(X)", "Y2 = 5 * X * cos(X)")

plotLine1 = plot("Y1", tLine, "Timestamp", "Y1").show()
```

![A ticking line plot](../../assets/tutorials/crash-course/crash-course-6.gif)

Multiple series can be plotted together in the same figure.

```groovy test-set=1 ticking-table order=null
plotLine2 = figure()
    .plot("Y1", tLine, "Timestamp", "Y1")
    .plot("Y2", tLine, "Timestamp", "Y2")
    .show()
```

![A ticking plot with multiple series](../../assets/tutorials/crash-course/crash-course-7.gif)

## Plots with multiple axes

[`twinX`](https://deephaven.io/core/javadoc/io/deephaven/plot/Figure.html#twinX()) and [`twinY`](https://deephaven.io/core/javadoc/io/deephaven/plot/Figure.html#twinY()) allow you to create plots with multiple `x` or `y` axes. For example, you can use [`twinX`](https://deephaven.io/core/javadoc/io/deephaven/plot/Figure.html#twinX()) to create a plot with two different `y` axes but a shared `x` axis.

```groovy test-set=1 order=null
plotTwin = figure()
    .newChart()
    .plot("Y1", tLine, "Timestamp", "Y1")
    .twinX()
    .plot("Y2", tLine, "Timestamp", "Y2")
    .show()
```

![A ticking plot with two x axes](../../assets/tutorials/crash-course/crash-course-8.gif)

## Subplots

Figures can also contain more than just one plot. For instance, a single figure could contain two plots stacked on top of one another, side by side, four plots in a 2x2 grid, and so on. These subplots <!--TODO: add link to subplot doc when it exists in Groovy--> are arranged into a grid, and then placed into specific locations in the grid with [`newChart`](https://deephaven.io/core/javadoc/io/deephaven/plot/Figure.html#newChart()).

The example below creates a figure with two subplots side-by-side.

```groovy test-set=1 order=null
plotSub = figure(1, 2)
    .newChart(0, 0)
    .plot("Y1", tLine, "Timestamp", "Y1")
    .newChart(0, 1)
    .plot("Y2", tLine, "Timestamp", "Y2")
    .show()
```

![Two subplots, displayed side-by-side](../../assets/tutorials/crash-course/crash-course-9.gif)

Far more plots are available, including [histograms](../../how-to-guides/plotting/api-plotting.md#histogram), [pie charts](../../how-to-guides/plotting/api-plotting.md#pie), [scatter plots](../../how-to-guides/plotting/api-plotting.md#xy-series-as-a-scatter-plot), and more.
