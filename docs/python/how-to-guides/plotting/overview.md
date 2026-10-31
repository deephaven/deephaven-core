---
title: Plotting overview
---

In Deephaven, plots can be as live as your data. A [Deephaven Express](/core/plotly/docs/) figure updates by itself as its table ticks, so you don't poll for new data or rebuild the figure.

Deephaven supports several ways to plot table data. They differ most in how they handle [real-time (ticking) tables](../../conceptual/table-types.md), which update as new data arrives:

- [Deephaven Express](/core/plotly/docs/), built on top of [Plotly Express](https://plotly.com/python/plotly-express/), plots real-time data directly from Deephaven tables. For examples, see the [Deephaven Express plot types](/core/plotly/docs#plot-types).
- [Deephaven's built-in plotting API](./api-plotting.md) also plots ticking tables natively, but it is no longer under active development.
- [Matplotlib and Seaborn](./matplot-seaborn.md) plot a copy of the table's data. For ticking tables, the Matplotlib plugin's [`TableAnimation`](./matplot-seaborn.md#real-time-plotting-with-tableanimation) class copies the data and redraws the whole figure on each update. See [Limitations of copy-based plotting](#limitations-of-copy-based-plotting).
- [Plotly](https://plotly.com/python/) plots a snapshot, such as a pandas DataFrame from [`to_pandas`](../../reference/pandas/to-pandas.md), so the figure doesn't change when the table ticks.

For most plotting tasks, use [Deephaven Express](/core/plotly/docs/). If you'd rather build a chart from the UI without writing code, see [Chart Builder](../user-interface/chart-builder.md).

## Plot a ticking table

Let's plot a ticking table. This example uses [`time_table`](../../reference/table-operations/create/timeTable.md) to create a table that adds a row every second, then plots it with [`dx.line`](/core/plotly/docs/line/) from [Deephaven Express](/core/plotly/docs/). The line grows each time a new row arrives, with no extra code:

```python ticking-table order=null
from deephaven import time_table
import deephaven.plot.express as dx

source = time_table("PT1S").update("Y = Math.sin(0.1 * i)")
line_plot = dx.line(source, x="Timestamp", y="Y")
```

## Plotting library support

The table below compares the options for [ticking tables](../../conceptual/table-types.md) and for static tables, such as historical data. Each library name links to its documentation.

| Library                                                 | Ticking tables                                       | Static tables | Ease of use   | Installation                                           |
| ------------------------------------------------------- | ---------------------------------------------------- | ------------- | ------------- | ------------------------------------------------------ |
| **[Deephaven Express](/core/plotly/docs/)**             | **Updates natively**                                 | **Excellent** | **Excellent** | `pip install deephaven-plugin-plotly-express`\*        |
| **[Built-in plotting API (legacy)](./api-plotting.md)** | **Updates natively**                                 | Good          | Good          | **Built in**                                           |
| **[Matplotlib](./matplot-seaborn.md)**                  | [Full redraw](#limitations-of-copy-based-plotting)   | Good          | Fair          | `pip install deephaven-plugin-matplotlib`\*            |
| **[Seaborn](./matplot-seaborn.md)**                     | [Full redraw](#limitations-of-copy-based-plotting)   | Good          | Excellent     | `pip install "deephaven-plugin-matplotlib[seaborn]"`\* |
| **[Plotly](https://plotly.com/python/)**                | [Snapshot only](#limitations-of-copy-based-plotting) | Good          | Good          | Installed with [Deephaven Express](/core/plotly/docs/) |

<small>

\*The `ghcr.io/deephaven/server` Docker image includes [Deephaven Express](/core/plotly/docs/) and the [Matplotlib plugin](./matplot-seaborn.md). The machine-learning images don't include them, and neither does a pip install of Deephaven. To add them, see [Install and use plugins](../install-use-plugins.md). The `server-slim` image runs a Groovy console and has no Python, so it can't use either plugin.

</small>

## Limitations of copy-based plotting

[Matplotlib](./matplot-seaborn.md), [Seaborn](./matplot-seaborn.md), and [Plotly](https://plotly.com/python/) don't read Deephaven tables directly. They plot a copy of the data, which has two costs:

- **Memory and redraw time**: Each update copies table data into Python. By default, [`TableAnimation`](./matplot-seaborn.md#real-time-plotting-with-tableanimation) and [`to_pandas`](../../reference/pandas/to-pandas.md) copy every column. Pass only the columns you plot to reduce the copy. [`TableAnimation`](./matplot-seaborn.md#real-time-plotting-with-tableanimation) also redraws the whole figure on every update, so each update gets slower as the table grows.
- **Stale figures**: A Plotly figure built from a snapshot shows the data as it was when the snapshot was taken. To see new data, you must build the figure again.

[Deephaven Express](/core/plotly/docs/) subscribes to table updates directly, so its figures update as the table ticks without copying the whole table into Python on every change.

## Related documentation

- [Table types](../../conceptual/table-types.md)
- [Deephaven Express](/core/plotly/docs/)
- [Built-in plotting API](./api-plotting.md)
- [Matplotlib and Seaborn](./matplot-seaborn.md)
- [Install and use plugins](../install-use-plugins.md)
- [Chart Builder](../user-interface/chart-builder.md)
- [`time_table`](../../reference/table-operations/create/timeTable.md)
- [`to_pandas`](../../reference/pandas/to-pandas.md)
- [Pydoc](/core/pydoc/code/deephaven.pandas.html#deephaven.pandas.to_pandas)
