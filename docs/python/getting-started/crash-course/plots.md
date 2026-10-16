---
title: Real-time Plots
---

Whether your data is static or updating in real time, Deephaven supports plotting via multiple libraries, including [Deephaven Express](/core/plotly/docs/) and Deephaven's built-in [plotting API](/core/pydoc/code/deephaven.plot.html#module-deephaven.plot). Deephaven Express is a real-time version of [Plotly Express](https://plotly.com/python/plotly-express/).

> [!NOTE]
> Deephaven Express is included in the `ghcr.io/deephaven/server` Docker image used in this crash course. If you installed Deephaven with pip, install Deephaven Express with `pip install deephaven-plugin-plotly-express`.

## Deephaven Express

[Deephaven Express](/core/plotly/docs/), the recommended plotting library, builds real-time plots directly from Deephaven tables. To create a simple [line plot](https://en.wikipedia.org/wiki/Line_chart) that ticks in lockstep with the source table, pass the table and its column names to [`dx.line`](/core/plotly/docs/line/). Passing a list of columns to `y` plots each column as a separate line with its own legend entry:

```python test-set=1 ticking-table order=null
from deephaven import time_table
import deephaven.plot.express as dx

t_line = time_table("PT0.2s").update(
    ["X = 0.05 * ii", "Y1 = X * sin(X)", "Y2 = 5 * X * cos(X)"]
)

plot_dx = dx.line(t_line, x="Timestamp", y=["Y1", "Y2"])
```

Deephaven Express also puts series on [separate y axes](/core/plotly/docs/multiple-axes/) with the `yaxis_sequence` argument and arranges several plots in a grid of [subplots](/core/plotly/docs/sub-plots/) with `dx.make_subplots`.

## Built-in plotting API

Deephaven also has a built-in [plotting API](/core/pydoc/code/deephaven.plot.html#module-deephaven.plot). It is no longer under active development, so use Deephaven Express for new work. You'll still see the built-in API in existing queries, so the next sections show how it works.

A figure is the container that holds one or more plots. To plot `Y1` as a line with the built-in API, create a [`Figure`](/core/pydoc/code/deephaven.plot.figure.html#deephaven.plot.figure.Figure), add a series with [`plot_xy`](../../reference/plot/plot.md), and call [`show`](/core/pydoc/code/deephaven.plot.figure.html#deephaven.plot.figure.Figure.show) to display it:

```python test-set=1 ticking-table order=null
from deephaven.plot.figure import Figure

plot_line_1 = Figure().plot_xy(series_name="Y1", t=t_line, x="Timestamp", y="Y1").show()
```

![A ticking line plot](../../assets/tutorials/crash-course/crash-course-6.gif)

To plot both `Y1` and `Y2`, as the Deephaven Express example does, chain [`plot_xy`](../../reference/plot/plot.md) calls:

```python test-set=1 ticking-table order=null
plot_line_2 = (
    Figure()
    .plot_xy(series_name="Y1", t=t_line, x="Timestamp", y="Y1")
    .plot_xy(series_name="Y2", t=t_line, x="Timestamp", y="Y2")
    .show()
)
```

![A ticking plot with multiple series](../../assets/tutorials/crash-course/crash-course-7.gif)

### Plots with multiple axes

[`x_twin`](/core/pydoc/code/deephaven.plot.figure.html#deephaven.plot.figure.Figure.x_twin) adds a new set of axes that shares the current x axis, so the plot gets a second y axis. [`y_twin`](/core/pydoc/code/deephaven.plot.figure.html#deephaven.plot.figure.Figure.y_twin) does the same with a shared y axis, which gives a second x axis. The example below uses `x_twin` to plot `Y1` and `Y2` against one x axis, each with its own y axis.

```python test-set=1 ticking-table order=null
plot_twin = (
    Figure()
    .plot_xy(series_name="Y1", t=t_line, x="Timestamp", y="Y1")
    .x_twin()
    .plot_xy(series_name="Y2", t=t_line, x="Timestamp", y="Y2")
    .show()
)
```

![A ticking plot with two y axes that share an x axis](../../assets/tutorials/crash-course/crash-course-8.gif)

### Subplots

So far, each figure has held a single plot, even when that plot has several series. A figure can also hold several plots arranged in a grid. For instance, a single figure could contain two plots stacked vertically, two plots side by side, or four plots in a 2x2 grid. To create these [subplots](../../how-to-guides/plotting/api-plotting.md#subplots), set the grid size with the `rows` and `cols` arguments to [`Figure`](/core/pydoc/code/deephaven.plot.figure.html#deephaven.plot.figure.Figure), then place each plot in a cell with [`new_chart`](/core/pydoc/code/deephaven.plot.figure.html#deephaven.plot.figure.Figure.new_chart), whose `row` and `col` indices start at 0.

The example below creates a figure with two subplots side by side.

```python test-set=1 ticking-table order=null
plot_sub = (
    Figure(rows=1, cols=2)
    .new_chart(row=0, col=0)
    .plot_xy(series_name="Y1", t=t_line, x="Timestamp", y="Y1")
    .new_chart(row=0, col=1)
    .plot_xy(series_name="Y2", t=t_line, x="Timestamp", y="Y2")
    .show()
)
```

![Two subplots, displayed side by side](../../assets/tutorials/crash-course/crash-course-9.gif)

## More plot types

[Deephaven Express](/core/plotly/docs/) has its own [list of plot types](/core/plotly/docs#plot-types). The built-in [plotting API](/core/pydoc/code/deephaven.plot.html#module-deephaven.plot) also offers far more plot types, including [histograms](../../how-to-guides/plotting/api-plotting.md#histogram), [pie charts](../../how-to-guides/plotting/api-plotting.md#pie), and [scatter plots](../../how-to-guides/plotting/api-plotting.md#xy-series-as-a-scatter-plot). For other options, see the [plotting overview](../../how-to-guides/plotting/overview.md), which also links to a guide on plotting with [Matplotlib and Seaborn](../../how-to-guides/plotting/matplot-seaborn.md).

> [!TIP]
> Prefer a no-code approach? Use the [Chart Builder](../../how-to-guides/user-interface/chart-builder.md) to create line, bar, scatter, and pie charts from any table directly in the UI.
