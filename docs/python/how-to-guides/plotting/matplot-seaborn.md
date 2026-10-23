---
title: Matplotlib and Seaborn
---

> [!NOTE]
> For Pythonic plotting in Deephaven, we recommend [Deephaven Express](/core/plotly/docs/), a real-time version of Plotly Express. It plots [ticking tables](../../conceptual/table-types.md) directly, while the Matplotlib plugin copies the table's data and redraws the whole figure on every update. See [Limitations of copy-based plotting](./overview.md#limitations-of-copy-based-plotting).

This guide shows you how to use [Matplotlib](https://matplotlib.org/) and [Seaborn](https://seaborn.pydata.org/) with [Deephaven's Matplotlib plugin](https://pypi.org/project/deephaven-plugin-matplotlib/) to create plots in Deephaven, both static plots and plots that redraw each time a [ticking table](../../conceptual/table-types.md) updates.

> [!CAUTION]
> All examples in this guide use [Matplotlib's explicit interface](https://matplotlib.org/stable/users/explain/figure/api_interfaces.html). They call methods on figure and axes objects, such as `ax.plot`, instead of `plt.plot`. Use the explicit interface in your own code too, especially when several plots draw data from [ticking tables](../../conceptual/table-types.md). The implicit `plt` functions act on a global current figure, and another plot may have changed it.

## Install the Matplotlib plugin and Seaborn

What you need to install depends on how you run Deephaven:

- The `ghcr.io/deephaven/server` Docker image includes [Deephaven's Matplotlib plugin](https://pypi.org/project/deephaven-plugin-matplotlib/) but not Seaborn. To use Seaborn, add it as described in [Permanently add packages to a custom Docker image](../install-and-use-python-packages.md#permanently-add-packages-to-a-custom-docker-image).
- The machine-learning images (`server-all-ai`, `server-nltk`, `server-pytorch`, `server-sklearn`, and `server-tensorflow`) include neither, so add both the same way.
- In a pip-installed Deephaven environment, install them with `pip3`, as shown below.

The `server-slim` image runs a Groovy console and has no Python, so it can't use this plugin. For other ways to install plugins, see [Install and use plugins](../install-use-plugins.md).

To install the plugin with `pip3`, run:

```shell
pip3 install deephaven-plugin-matplotlib
```

To install Seaborn as well, run this command instead. The `seaborn` extra installs Seaborn 0.13.0 or later along with the plugin:

```shell
pip3 install "deephaven-plugin-matplotlib[seaborn]"
```

## Matplotlib examples

### Static plotting

The plugin displays each Matplotlib figure that you assign to a variable as a PNG image in its own panel in the Deephaven IDE. You don't need to call `plt.show`. This example plots one line and assigns the figure to `m_figure`:

```python skip-test
import matplotlib.pyplot as plt


x = [0, 2, 4, 6]
y = [1, 3, 4, 8]
m_figure, m_axes = plt.subplots()
m_axes.plot(x, y)
m_axes.set_xlabel("x values")
m_axes.set_ylabel("y values")
m_axes.set_title("plotted x and y values")
m_axes.legend(["line 1"])
```

![A line plot of y values against x values](../../assets/how-to/matplot/plot.png)

Matplotlib's full plotting functionality is available in the Deephaven IDE. For example, a figure with several subplots appears as a single image when you assign it to a variable:

```python skip-test
import matplotlib.pyplot as plt
import numpy as np
import math

# Angles from 0 to 2π radians, as a NumPy array
x = np.arange(0, math.pi * 2, 0.05)

# Compute sine, cosine, tangent, and hyperbolic tangent values for each angle
y_sin = np.sin(x)
y_cos = np.cos(x)
y_tan = np.tan(x)
y_tanh = np.tanh(x)

# Create a figure with a 2x2 grid of axes; `axis` is a 2D array indexed as axis[row, column]
figure, axis = plt.subplots(2, 2)

# For Sine Function
axis[0, 0].plot(x, y_sin)
axis[0, 0].set_title("Sine Function")

# For Cosine Function
axis[0, 1].plot(x, y_cos)
axis[0, 1].set_title("Cosine Function")

# For Tangent Function
axis[1, 0].plot(x, y_tan)
axis[1, 0].set_title("Tangent Function")

# For Tanh Function
axis[1, 1].plot(x, y_tanh)
axis[1, 1].set_title("Tanh Function")
```

![Four plots in a two-by-two grid](../../assets/how-to/matplot/stacked_plot.png)

To plot a Deephaven table, convert it to a pandas DataFrame with [`to_pandas`](../../reference/pandas/to-pandas.md) and pass its columns to Matplotlib:

```python skip-test
import matplotlib.pyplot as plt
from deephaven import empty_table
from deephaven import pandas as dhpd

source = empty_table(10).update(["X = i", "Y = X * X"])
df = dhpd.to_pandas(source)

table_figure, table_axes = plt.subplots()
table_axes.plot(df["X"], df["Y"])
table_axes.set_title("Y against X from a Deephaven table")
```

The [3D plots](#3d-plots) below also pass DataFrame columns to Matplotlib, but they read the DataFrame from a CSV file.

#### 3D plots

To draw a 3D plot, create axes with `projection="3d"` and assign the figure to a variable. The plugin displays it as an image in its own panel, like any other figure. We adapted the examples below from [Five advanced plots in Python Matplotlib](https://regenerativetoday.com/five-advanced-plots-in-python-matplotlib/). They use the [auto85 dataset from Kaggle](https://www.kaggle.com/datasets/fazilbtopal/auto85).

To run them, download the dataset's CSV file and save it as `auto_clean.csv` in your Deephaven data directory. For more information, see the guide on [Docker data volumes](../../conceptual/docker-data-volumes.md#the-data-mount-point). If you run Deephaven from pip, change `/data/auto_clean.csv` to the path where you saved the file.

The first example reads the file into a pandas DataFrame, `df`, and plots each car's length, width, and height as a 3D scatter plot. It colors each point by the car's peak RPM and sizes it by the car's price:

```python skip-test
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt


df = pd.read_csv("/data/auto_clean.csv")

fig = plt.figure(figsize=(10, 10))
ax = fig.add_subplot(projection="3d")
ax.scatter3D(
    df["length"],
    df["width"],
    df["height"],
    c=df["peak-rpm"],
    s=df["price"] / 50,
    alpha=0.4,
)
ax.set_xlabel("Length")
ax.set_ylabel("Width")
ax.set_zlabel("Height")
ax.set_title("Relationship between height, width, and length")
```

![A 3D scatter plot](../../assets/how-to/matplot/scatter.png)

The next two examples reuse `df` and `plt` from the previous example. The first one also reuses `np` and draws a 3D bar graph of the mean peak RPM and mean price for each body style:

```python skip-test
df["body_style1"] = df["body-style"].map(
    {"convertible": 1, "hatchback": 2, "sedan": 3, "wagon": 4, "hardtop": 5}
)

gr = df.groupby("body_style1")[["peak-rpm", "price"]].agg("mean")
x = gr.index
y = gr["peak-rpm"]
z = [0] * 5
dx = 0.3 * np.ones_like(z)
dy = [30] * 5
dz = gr["price"]
fig = plt.figure(figsize=(10, 8))
ax = fig.add_subplot(111, projection="3d")
ax.set_xticks(x, labels=["convertible", "hatchback", "sedan", "wagon", "hardtop"])
ax.set_xlabel("Body Style", labelpad=7)
ax.set_yticks(np.linspace(5000, 5250, 6))
ax.set_ylabel("Peak Rpm", labelpad=10)
ax.set_zlabel("Price")
ax.set_zticks(np.linspace(7000, 22250, 6))
ax.set_title("Mean price and mean peak RPM by body style")
ax.bar3d(x, y, z, dx, dy, dz)
```

![A 3D bar graph](../../assets/how-to/matplot/3d_bar.png)

The last example draws a triangular surface plot of each car's price over its peak RPM and city MPG:

```python skip-test
tri_surf = plt.figure(figsize=(8, 8))
ax = tri_surf.add_subplot(projection="3d")
x = df["peak-rpm"]
y = df["city-mpg"]
z = df["price"]
ax.plot_trisurf(x, y, z, cmap="viridis", edgecolor="none")
ax.set_xlabel("Peak RPM")
ax.set_ylabel("City-MPG")
ax.set_zlabel("Price")
ax.set_title("Price by peak RPM and city MPG")
ax.view_init(60, 25)
```

![A 3D triangular surface plot](../../assets/how-to/matplot/tri_surf.png)

### Real-time plotting with `TableAnimation`

[Deephaven's Matplotlib plugin](https://pypi.org/project/deephaven-plugin-matplotlib/) also lets you visualize real-time data: its `TableAnimation` class redraws a figure each time a [ticking table](../../conceptual/table-types.md) updates.

`TableAnimation` needs three inputs:

1. The figure to redraw.
2. The ticking table that supplies the data.
3. The function that updates the figure. `TableAnimation` calls it with two arguments:
   - `data`: a dictionary that maps each column name to a NumPy array of the table's current values.
   - `update`: the [`TableUpdate`](/core/pydoc/code/deephaven.table_listener.html#deephaven.table_listener.TableUpdate) that triggered the call.

By default, `data` holds every column of the table, and `TableAnimation` copies all of them into NumPy arrays on each update. To copy fewer columns, pass their names in the optional `columns` argument, for example `TableAnimation(line_fig, tt_sin, update_line, columns=["X", "Y"])`.

The table must be a refreshing table. `TableAnimation` first calls the update function on the table's next update, so the figure stays empty until the table ticks.

Assign each `TableAnimation` to a variable, as the examples below do. If nothing references it, Python garbage-collects it and the animation stops.

Each example below uses its own variable names, such as `line_fig` and `update_line`. An update function looks up names like these each time it runs. If a later example reused a name, an earlier animation would start updating the later example's object instead of its own.

#### Line plot

This example plots a sine wave from a [time table](../time-table.md) that adds a row every second:

```python skip-test
import matplotlib.pyplot as plt
from deephaven import time_table
from deephaven.plugin.matplotlib import TableAnimation

# Create a ticking table with the sine function
tt_sin = time_table("PT1S").update(["X = 0.2 * i", "Y = Math.sin(X)"])

line_fig = plt.figure()  # Create a new figure
line_ax = line_fig.subplots()  # Add an axes to the figure
# Plot a line. Start with empty data, which table updates fill in.
(sine_line,) = line_ax.plot([], [])


# Define our update function. We only look at `data` here as the data is already stored in the format we want
def update_line(data, update):
    sine_line.set_data([data["X"], data["Y"]])

    # Resize and scale the axes. Our data may have expanded and we don't want it to appear off screen.
    line_ax.relim()
    line_ax.autoscale_view()


# Create our animation. It listens for updates on `tt_sin` and calls `update_line` whenever there is an update
line_ani = TableAnimation(line_fig, tt_sin, update_line, columns=["X", "Y"])
```

![A real-time line chart](../../assets/how-to/real_time_line_chart.gif)

#### Bar plot

This example plots the five largest `Y` values in a [ticking table](../../conceptual/table-types.md). Each time the table updates, `update_bar` sets the bar heights and labels each bar with its `X` value:

```python skip-test
import matplotlib.pyplot as plt
from deephaven import time_table
from deephaven.plugin.matplotlib import TableAnimation
from deephaven import SortDirection

top_n = 5
# Create a ticking table with the linear function y = x
tt_bar = time_table("PT1S").update(["X = i", "Y = i"])
tt_bar_sorted = tt_bar.sort(order_by=["Y"], order=[SortDirection.DESCENDING])
# Create a table with the 5 largest values for the chart
tt_bar_top = tt_bar_sorted.head(top_n)

bar_fig, bar_ax = plt.subplots()
bar_rects = bar_ax.bar(range(top_n), [0] * top_n)


def update_bar(data, update):
    # Update the heights of the bars
    for rect, h in zip(bar_rects, data["Y"]):
        rect.set_height(h)
    # Update the labels, one per bar that has data
    bar_ax.set_xticks(range(len(data["X"])), labels=data["X"])
    bar_ax.relim()
    bar_ax.autoscale_view()


bar_ani = TableAnimation(bar_fig, tt_bar_top, update_bar, columns=["X", "Y"])
```

![A bar chart updating in real time](../../assets/how-to/real_time_column_chart.gif)

#### Scatter plot

A scatter plot doesn't store separate x and y arrays like the [line plot](#line-plot). It stores each point as an offset, which is an (x, y) pair, and a size. This example's `update_scatter` keeps one list of offsets and one of sizes. Each time the table updates, it:

1. Reads the rows added since the last update with [`added`](/core/pydoc/code/deephaven.table_listener.html#deephaven.table_listener.TableUpdate.added).
2. Appends each row's `X` and `Y` to the offsets and its `Z` to the sizes.
3. Passes the lists to `set_offsets` and `set_sizes`.

Because it only appends, this approach works only for an [add-only table](../../conceptual/table-types.md#specialization-2-add-only), which never removes or modifies rows. It also plots only rows added after you create the `TableAnimation`, because the rows already in the table never reach the update function as added rows. The [time table](../time-table.md) in this example is add-only and starts empty.

`update_scatter` doesn't use `data`, so the example passes `columns=[]` and `TableAnimation` copies no columns:

```python skip-test
import matplotlib.pyplot as plt
from deephaven import time_table
from deephaven.plugin.matplotlib import TableAnimation

tt_scatter = time_table("PT1S").update(
    ["X = Math.random()", "Y = Math.random()", "Z = Math.random() * 50"]
)

scatter_fig = plt.figure()
scatter_ax = scatter_fig.subplots()
scatter_ax.set_xlim(0, 1)
scatter_ax.set_ylim(0, 1)
scatter_points = scatter_ax.scatter([], [])  # Provide empty data initially
scatter_offsets = []  # Store separate lists of offsets and sizes
scatter_sizes = []


def update_scatter(data, update):
    # This assumes that rows are only ever added to the table. Otherwise, you also need
    # the removed and modified rows from `update`
    added = update.added()
    for i in range(0, len(added["X"])):
        # Append new data to the sources
        scatter_offsets.append([added["X"][i], added["Y"][i]])
        scatter_sizes.append(added["Z"][i])

    # Update the figure
    scatter_points.set_offsets(scatter_offsets)
    scatter_points.set_sizes(scatter_sizes)


scatter_ani = TableAnimation(scatter_fig, tt_scatter, update_scatter, columns=[])
```

![A real-time scatter plot](../../assets/how-to/real_time_scatter.gif)

If your table can remove or modify rows, rebuild the points from `data` on every update instead of appending. The next example keeps only the 20 most recent rows with [`tail`](../../reference/table-operations/filter/tail.md). Once the table holds 20 rows, it removes its oldest row each time it adds a new one. Compared with the previous example, it:

- Passes `columns=["X", "Y", "Z"]` instead of `columns=[]`, so that `TableAnimation` copies those columns into `data`.
- Builds the offsets from `data["X"]` and `data["Y"]` with NumPy's `column_stack`, and the sizes from `data["Z"]`.

```python skip-test
import matplotlib.pyplot as plt
import numpy as np
from deephaven import time_table
from deephaven.plugin.matplotlib import TableAnimation

# Keep only the 20 most recent rows, so old rows are removed as new rows arrive
tt_recent = (
    time_table("PT1S")
    .update(["X = Math.random()", "Y = Math.random()", "Z = Math.random() * 50"])
    .tail(20)
)

recent_fig = plt.figure()
recent_ax = recent_fig.subplots()
recent_ax.set_xlim(0, 1)
recent_ax.set_ylim(0, 1)
recent_points = recent_ax.scatter([], [])


def update_recent(data, update):
    # Rebuild every point from the table's current rows
    recent_points.set_offsets(np.column_stack([data["X"], data["Y"]]))
    recent_points.set_sizes(data["Z"])


recent_ani = TableAnimation(
    recent_fig, tt_recent, update_recent, columns=["X", "Y", "Z"]
)
```

#### Multiple series

A figure can hold more than one kind of series. This example's `update_series` redraws the lines from `data`, as in the [line plot](#line-plot). It adds the scatter points incrementally, as in the [scatter plot](#scatter-plot).

Because of that, its scatter points have the same limits as the scatter plot's: the table must be add-only, and only rows added after you create the `TableAnimation` appear. For other tables, rebuild the scatter points from `data` on each update, as in the [example that keeps the 20 most recent rows](#scatter-plot).

This example draws two line plots and a scatter plot on the same axes:

```python skip-test
import matplotlib.pyplot as plt
from deephaven import time_table
from deephaven.plugin.matplotlib import TableAnimation

tt_series = time_table("PT1S").update(
    [
        "X = i",
        "Y = Math.sin(X)",
        "Z = Math.cos(X)",
        "R = Math.random()",
        "S = Math.random() * 100",
    ]
)

series_fig = plt.figure()
series_ax = series_fig.subplots()
(series_line1,) = series_ax.plot([], [])
(series_line2,) = series_ax.plot([], [])
series_points = series_ax.scatter([], [])
series_offsets = []
series_sizes = []


def update_series(data, update):
    series_line1.set_data([data["X"], data["Y"]])
    series_line2.set_data([data["X"], data["Z"]])
    added = update.added()
    for i in range(0, len(added["X"])):
        series_offsets.append([added["X"][i], added["R"][i]])
        series_sizes.append(added["S"][i])
    series_points.set_offsets(series_offsets)
    series_points.set_sizes(series_sizes)
    series_ax.relim()
    series_ax.autoscale_view()


series_ani = TableAnimation(
    series_fig, tt_series, update_series, columns=["X", "Y", "Z"]
)
```

![Multiple series on the same plot](../../assets/how-to/real_time_multiple_series.gif)

## Seaborn examples

### Static plotting with Seaborn

Seaborn's [axes-level functions](https://seaborn.pydata.org/tutorial/function_overview.html), such as `lineplot`, `scatterplot`, and `barplot`, draw on Matplotlib axes. The Matplotlib plugin therefore displays them the same way it displays a Matplotlib figure. Create a figure and axes with `plt.subplots`, pass the axes to Seaborn's `ax` argument, and assign the figure to a variable:

```python skip-test
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd


x = [0, 2, 4, 6]
y = [1, 3, 4, 8]
df = pd.DataFrame({"X": x, "Y": y})
m_figure, m_axes = plt.subplots()
sns.lineplot(data=df, x="X", y="Y", ax=m_axes)
m_axes.set_xlabel("x values")
m_axes.set_ylabel("y values")
m_axes.set_title("plotted x and y values")
m_axes.legend(["line 1"])
```

![A line plot created with Seaborn](../../assets/how-to/seaborn.png)

[Figure-level functions](https://seaborn.pydata.org/tutorial/function_overview.html), such as `relplot` and `catplot`, return a [`FacetGrid`](https://seaborn.pydata.org/generated/seaborn.FacetGrid.html) instead of a figure. To display one, assign its `figure` attribute to a variable, for example `rel_figure = sns.relplot(...).figure`.

### Real-time plotting with Seaborn

The real-time Seaborn examples work like the [Matplotlib real-time examples](#real-time-plotting-with-tableanimation). Each one passes a figure, a [ticking table](../../conceptual/table-types.md), and an update function to `TableAnimation`, which calls the function each time the table updates. Each example also uses its own variable names, so running a later example doesn't change what an earlier animation updates.

Each update function below clears the axes, builds a pandas DataFrame from `data`, and redraws the plot. Each example passes `columns`, so `TableAnimation` copies only the columns that the plot uses.

Don't call [`to_pandas`](../../reference/pandas/to-pandas.md) on the table inside the update function. `TableAnimation` runs the update function from a [table listener](../table-listeners-python.md), and `to_pandas` performs a table operation, which a listener shouldn't do. `data` already holds a copy of the columns you need.

#### Seaborn line plot

This example plots a cosine wave from a [time table](../time-table.md) that adds a row every second:

```python skip-test
from deephaven.plugin.matplotlib import TableAnimation
from deephaven import time_table

import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns

# Create a ticking table with the cosine function
tt_cos = time_table("PT1S").update(["X = 0.2 * i", "Y = Math.cos(X)"])

sns_line_fig, sns_line_ax = plt.subplots()  # Create a new figure


# This function updates a figure
def update_sns_line(data, update):
    # Clear the axes (don't draw over old lines)
    sns_line_ax.clear()
    # Draw the line plot from the columns TableAnimation copied into `data`
    sns.lineplot(pd.DataFrame(data), x="X", y="Y", ax=sns_line_ax)


# Create our animation. It listens for updates on `tt_cos` and calls `update_sns_line` whenever there is an update
sns_line_ani = TableAnimation(sns_line_fig, tt_cos, update_sns_line, columns=["X", "Y"])
```

![A real-time line plot created with Seaborn](../../assets/how-to/seaborn_line.gif)

#### Seaborn bar plot

This example plots the five largest `Y` values in a [ticking table](../../conceptual/table-types.md):

```python skip-test
from deephaven.plugin.matplotlib import TableAnimation
from deephaven import SortDirection
from deephaven import time_table

import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns

top_n = 5

# Create a ticking table with the linear function y = x
tt_linear = time_table("PT1S").update(["X = i", "Y = i"])
# Sort `tt_linear` in descending order by the Y column
tt_linear_sorted = tt_linear.sort(order_by=["Y"], order=[SortDirection.DESCENDING])
# Create a table with the 5 largest values for the chart
tt_linear_top = tt_linear_sorted.head(top_n)

sns_bar_fig, sns_bar_ax = plt.subplots()


# This function updates a figure
def update_sns_bar(data, update):
    # Clear the axes (don't draw over old bars)
    sns_bar_ax.clear()
    # Draw the bar plot from the top 5 rows that TableAnimation copied into `data`
    sns.barplot(pd.DataFrame(data), x="X", y="Y", ax=sns_bar_ax)


# Create our animation. It listens for updates on `tt_linear_top` and calls `update_sns_bar` whenever there is an update
sns_bar_ani = TableAnimation(
    sns_bar_fig, tt_linear_top, update_sns_bar, columns=["X", "Y"]
)
```

![A real-time bar plot created with Seaborn](../../assets/how-to/seaborn_bar.gif)

#### Seaborn scatter plot

This example plots random points and sizes each point by its `Z` value:

```python skip-test
from deephaven.plugin.matplotlib import TableAnimation
from deephaven import time_table

import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns

# Create a ticking table with random x, y, and z values
tt_random = time_table("PT2S").update(
    ["X = Math.random()", "Y = Math.random()", "Z = Math.random() * 50"]
)

sns_scatter_fig, sns_scatter_ax = plt.subplots()


# This function updates a figure
def update_sns_scatter(data, update):
    # Clear the axes (don't draw over old points)
    sns_scatter_ax.clear()
    # Draw the scatter plot from the columns TableAnimation copied into `data`
    sns.scatterplot(pd.DataFrame(data), x="X", y="Y", size="Z", ax=sns_scatter_ax)


# Create our animation. It listens for updates on `tt_random` and calls `update_sns_scatter` whenever there is an update
sns_scatter_ani = TableAnimation(
    sns_scatter_fig, tt_random, update_sns_scatter, columns=["X", "Y", "Z"]
)
```

![A real-time scatter plot created with Seaborn](../../assets/how-to/seaborn_scatter.gif)

#### Seaborn multiple series

This example draws two line plots and a scatter plot on the same axes:

```python skip-test
from deephaven.plugin.matplotlib import TableAnimation
from deephaven import time_table

import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns

# Create a ticking table with sine, cosine, and random numbers
tt_multi = time_table("PT1S").update(
    [
        "X = i",
        "Y = Math.sin(X)",
        "Z = Math.cos(X)",
        "R = Math.random()",
        "S = Math.random() * 50",
    ]
)

sns_multi_fig, sns_multi_ax = plt.subplots()


# This function updates a figure
def update_sns_multi(data, update):
    # Clear the axes so new plots don't draw over old ones
    sns_multi_ax.clear()
    df = pd.DataFrame(data)
    # Draw two line plots and a scatterplot
    sns.lineplot(df, x="X", y="Y", ax=sns_multi_ax)
    sns.lineplot(df, x="X", y="Z", ax=sns_multi_ax)
    sns.scatterplot(df, x="X", y="R", size="S", ax=sns_multi_ax)


# Create our animation. It listens for updates on `tt_multi` and calls `update_sns_multi` whenever there is an update
sns_multi_ani = TableAnimation(
    sns_multi_fig, tt_multi, update_sns_multi, columns=["X", "Y", "Z", "R", "S"]
)
```

![A multi-series line plot created with Seaborn](../../assets/how-to/seaborn_multipleplots.gif)

## Related documentation

- [Deephaven's table types](../../conceptual/table-types.md)
- [Docker data volumes](../../conceptual/docker-data-volumes.md)
- [Plotting overview](./overview.md)
- [Install and use plugins](../install-use-plugins.md)
- [Install and use Python packages](../install-and-use-python-packages.md)
- [Use pandas in Deephaven queries](../use-pandas.md)
- [Create a time table](../time-table.md)
- [Listen to ticking tables](../table-listeners-python.md)
- [Deephaven Express](/core/plotly/docs/)
- [`empty_table`](../../reference/table-operations/create/emptyTable.md)
- [`head`](../../reference/table-operations/filter/head.md)
- [`sort`](../../reference/table-operations/sort/sort.md)
- [`tail`](../../reference/table-operations/filter/tail.md)
- [`time_table`](../../reference/table-operations/create/timeTable.md)
- [`to_pandas`](../../reference/pandas/to-pandas.md)
- [`update`](../../reference/table-operations/select/update.md)
- [`TableUpdate` Pydoc](/core/pydoc/code/deephaven.table_listener.html#deephaven.table_listener.TableUpdate)
- [`to_pandas` Pydoc](/core/pydoc/code/deephaven.pandas.html#deephaven.pandas.to_pandas)
