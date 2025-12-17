---
title: Real-time Plots
sidebar_label: Real-time Plots
---

Whether your data is static or updating in real time, Deephaven supports plotting via multiple libraries, including its own built-in [plotting API](/core/pydoc/code/deephaven.plot.html#module-deephaven.plot).

## Basic plots

> [!NOTE]
> Plotting with pip-installed Deephaven from Jupyter requires some additional setup you can read about [here](../../how-to-guides/jupyter.md).

Deephaven's native plotting library supports many common plot types. To create a simple [line plot](https://en.wikipedia.org/wiki/Line_chart) that ticks in lock-step with the source table:

```python test-set=1 ticking-table order=null
from deephaven import time_table
from deephaven.plot.figure import Figure

t_line = time_table("PT0.2s").update(
    ["X = 0.05 * ii", "Y1 = X * sin(X)", "Y2 = 5 * X * cos(X)"]
)

plot_line_1 = Figure().plot_xy(series_name="Y1", t=t_line, x="Timestamp", y="Y1").show()
```

![A ticking line plot](../../assets/tutorials/crash-course/crash-course-6.gif)

Multiple series can be plotted together in the same figure.

```python test-set=1 ticking-table order=null
plot_line_2 = (
    Figure()
    .plot_xy(series_name="Y1", t=t_line, x="Timestamp", y="Y1")
    .plot_xy(series_name="Y2", t=t_line, x="Timestamp", y="Y2")
    .show()
)
```

![A ticking plot with multiple series](../../assets/tutorials/crash-course/crash-course-7.gif)

## Plots with multiple axes

[`x_twin`](/core/pydoc/code/deephaven.plot.figure.html#deephaven.plot.figure.Figure.x_twin) and [`y_twin`](/core/pydoc/code/deephaven.plot.figure.html#deephaven.plot.figure.Figure.y_twin) allow you to create plots with multiple `x` or `y` axes. For example, you can use [`x_twin`](/core/pydoc/code/deephaven.plot.figure.html#deephaven.plot.figure.Figure.x_twin) to create a plot with two different `y` axes but a shared `x` axis.

```python test-set=1 order=null
plot_twin = (
    Figure()
    .new_chart()
    .plot_xy(series_name="Y1", t=t_line, x="Timestamp", y="Y1")
    .x_twin()
    .plot_xy(series_name="Y2", t=t_line, x="Timestamp", y="Y2")
    .show()
)
```

![A ticking plot with two x axes](../../assets/tutorials/crash-course/crash-course-8.gif)

## Subplots

Figures can also contain more than just one plot. For instance, a single figure could contain two plots stacked on top of one another, side by side, four plots in a 2x2 grid, and so on. These [subplots](../../how-to-guides/plotting/api-plotting.md#subplots) are arranged into a grid, and then placed into specific locations in the grid with [`new_chart`](/core/pydoc/code/deephaven.plot.figure.html#deephaven.plot.figure.Figure.new_chart).

The example below creates a figure with two subplots side-by-side.

```python test-set=1 order=null
plot_sub = (
    Figure(rows=1, cols=2)
    .new_chart(row=0, col=0)
    .plot_xy(series_name="Y1", t=t_line, x="Timestamp", y="Y1")
    .new_chart(row=0, col=1)
    .plot_xy(series_name="Y2", t=t_line, x="Timestamp", y="Y2")
    .show()
)
```

![Two subplots, displayed side-by-side](../../assets/tutorials/crash-course/crash-course-9.gif)

Far more plots are available, including [histograms](../../how-to-guides/plotting/api-plotting.md#histogram), [pie charts](../../how-to-guides/plotting/api-plotting.md#pie), [scatter plots](../../how-to-guides/plotting/api-plotting.md#xy-series-as-a-scatter-plot), and more. Deephaven also offers integrations with [Plotly-express](https://plotly.com/python/plotly-express/), [Matplotlib](https://matplotlib.org/), and [Seaborn](https://seaborn.pydata.org/) that are under active development.
