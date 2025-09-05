---
title: Matplotlib and Seaborn
---

> [!WARNING]
> For Pythonic plotting in Deephaven, we recommend [Deephaven Express](/core/plotly/docs/), a real-time version of Plotly Express. It has built-in support for ticking data, more active development, and fewer potential pitfalls when working with ticking plots.

This guide shows you how to use [Matplotlib](https://matplotlib.org/) and [Seaborn](https://seaborn.pydata.org/) to create plots in Deephaven.

## Install the Matplotlib plugin

Use `pip` to install Deephaven's Matplotlib plugin.

```shell
pip3 install deephaven-plugin-matplotlib
```

To include Seaborn, run:

```shell
pip3 install deephaven-plugin-matplotlib seaborn
```

The process for installing Python packages depends on whether Deephaven is run from Docker or Pip. See the guide on [installing Python packages](../../how-to-guides/install-and-use-python-packages.md) to learn more.

## Matplotlib examples

### Static plotting

> [!CAUTION]
> All examples in this guide use [Matplotlib's explicit interface](https://matplotlib.org/stable/users/explain/figure/api_interfaces.html#api-interfaces). Users should do the same, especially in examples where multiple plots source data from ticking tables.

Here is the basic usage of Matplotlib to show one figure:

```python skip-test
import matplotlib.pyplot as plt


x = [0, 2, 4, 6]
y = [1, 3, 4, 8]
m_figure, m_axes = plt.subplots()
plt.plot(x, y)
plt.xlabel("x values")
plt.ylabel("y values")
plt.title("plotted x and y values")
plt.legend(["line 1"])
```

![The above plot](../../assets/how-to/matplot/plot.png)

The full functionality of Matplotlib is avilable inside the Deephaven IDE:

```python skip-test
import matplotlib.pyplot as plt
import numpy as np
import math

# Get the angles from 0 to 2 pie (360 degree) in narray object
X = np.arange(0, math.pi * 2, 0.05)

# Using built-in trigonometric function we can directly plot
# the given cosine wave for the given angles
Y1 = np.sin(X)
Y2 = np.cos(X)
Y3 = np.tan(X)
Y4 = np.tanh(X)

# Initialise the subplot function using number of rows and columns
figure, axis = plt.subplots(2, 2)

# For Sine Function
axis[0, 0].plot(X, Y1)
axis[0, 0].set_title("Sine Function")

# For Cosine Function
axis[0, 1].plot(X, Y2)
axis[0, 1].set_title("Cosine Function")

# For Tangent Function
axis[1, 0].plot(X, Y3)
axis[1, 0].set_title("Tangent Function")

# For Tanh Function
axis[1, 1].plot(X, Y4)
axis[1, 1].set_title("Tanh Function")
```

![Four plots displayed side-by-side](../../assets/how-to/matplot/stacked_plot.png)

#### 3D plots

Here are [some 3D examples](https://regenerativetoday.com/five-advanced-plots-in-python-matplotlib/). The data, available from [Kaggle](https://www.kaggle.com/datasets/fazilbtopal/auto85), needs to be placed in the data directory. For more information see our guide on [Docker data volumes](../../conceptual/docker-data-volumes.md#the-data-mount-point).

```python skip-test
import pandas as pd
import numpy as np
from mpl_toolkits import mplot3d
import matplotlib.pyplot as plt


df = pd.read_csv("/data/auto_clean.csv")

fig = plt.figure(figsize=(10, 10))
ax = plt.axes(projection="3d")
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
ax.set_title("Relationship between height, weight, and length")
```

![A 3D scatter plot](../../assets/how-to/matplot/scatter.png)

```python skip-test
df["body-style"].unique()

df["body_style1"] = df["body-style"].replace(
    {"convertible": 1, "hatchback": 2, "sedan": 3, "wagon": 4, "hardtop": 5}
)

gr = df.groupby("body_style1")[["peak-rpm", "price"]].agg("mean")
x = gr.index
y = gr["peak-rpm"]
z = [0] * 5
colors = ["b", "g", "crimson", "r", "pink"]
dx = 0.3 * np.ones_like(z)
dy = [30] * 5
dz = gr["price"]
fig = plt.figure(figsize=(10, 8))
ax = fig.add_subplot(111, projection="3d")
ax.set_xticklabels(["convertible", "hatchback", "sedan", "wagon", "hardtop"])
ax.set_xlabel("Body Style", labelpad=7)
ax.set_yticks(np.linspace(5000, 5250, 6))
ax.set_ylabel("Peak Rpm", labelpad=10)
ax.set_zlabel("Price")
ax.set_zticks(np.linspace(7000, 22250, 6))
ax.set_title("Change of Price with Body_style and Peak RPM")
ax.bar3d(x, y, z, dx, dy, dz)
```

![A 3D bar graph](../../assets/how-to/matplot/3d_bar.png)

```python skip-test
def z_function(x, y):
    return np.sin(np.sqrt(x**2 + y**2))


tri_surf = plt.figure(figsize=(8, 8))
ax = plt.axes(projection="3d")
x = df["peak-rpm"]
y = df["city-mpg"]
z = z_function(x, y)
ax.plot_trisurf(x, y, z, cmap="viridis", edgecolor="none")
ax.set_xlabel("Peak RPM")
ax.set_ylabel("City-MPG")
ax.set_title("Peak RPM vs City-MPG")
ax.view_init(60, 25)
```

![The `tri_surf` 3D graph](../../assets/how-to/matplot/tri_surf.png)

### Real-time plotting with TableAnimation

[Deephaven's matplotlib plugin](https://pypi.org/project/deephaven-plugin-matplotlib/) also enables you to visualize real-time data.

To install the plugin, run:

```
pip install deephaven-plugin-matplotlib
```

Making a live animation requires two imports:

```python skip-test
from deephaven.plugin.matplotlib import TableAnimation
import matplotlib.pyplot as plt
```

The `TableAnimation` class will re-draw a plot every time a ticking table is updated. It needs three inputs:

1. The figure containing data (in the examples below, it's `fig`).
2. The table containing data (in the examples below, it's `tt` or `tt_sorted_top`).
3. The function defining how the plot is animated (`update_fig`).

See the examples below.

#### Line plot

```python skip-test
import matplotlib.pyplot as plt
from deephaven import time_table
from deephaven.plugin.matplotlib import TableAnimation

# Create a ticking table with the sin function
tt = time_table("PT1S").update(["x=i", "y=Math.sin(x)"])

fig = plt.figure()  # Create a new figure
ax = fig.subplots()  # Add an axes to the figure
(line,) = ax.plot(
    [], []
)  # Plot a line. Start with empty data, will get updated with table updates.

# Note that Deephaven Express eliminates the complexity of update_fig and TableAnimation


# Define our update function. We only look at `data` here as the data is already stored in the format we want
def update_fig(data, update):
    line.set_data([data["x"], data["y"]])

    # Resize and scale the axes. Our data may have expanded and we don't want it to appear off screen.
    ax.relim()
    ax.autoscale_view(True, True, True)


# Create our animation. It will listen for updates on `tt` and call `update_fig` whenever there is an update
ani = TableAnimation(fig, tt, update_fig)
```

![A real-time line chart](../../assets/how-to/real_time_line_chart.gif)

#### Bar plot

```python skip-test
import matplotlib.pyplot as plt
from deephaven import time_table
from deephaven.plugin.matplotlib import TableAnimation
from deephaven import SortDirection

top_n = 5
# Create a ticking table with the linear function y = x
tt = time_table("PT1S").update(["x=i", "y=i"])
tt_sorted = tt.sort(order_by=["y"], order=[SortDirection.DESCENDING])
# Create a table with 5 largest values for the chart
tt_sorted_top = tt_sorted.head(top_n)

fig, ax = plt.subplots()
rects = ax.bar(range(top_n), [0] * top_n)

# Note that Deephaven Express eliminates the complexity of update_fig and TableAnimation


def update_fig(data, update):
    # update heights of the columns
    for rect, h in zip(rects, data["y"]):
        rect.set_height(h)
    # update labels
    ax.set_xticklabels(data["x"])
    ax.relim()
    ax.autoscale_view(True, True, True)


bar_plot_ani = TableAnimation(fig, tt_sorted_top, update_fig)
```

![A bar chart updating in real time](../../assets/how-to/real_time_column_chart.gif)

#### Scatter plot

Scatter plots require data in a different format than line plots, so we need to pass in the data differently:

```python skip-test
import matplotlib.pyplot as plt
from deephaven import time_table
from deephaven.plugin.matplotlib import TableAnimation

tt = time_table("PT1S").update(
    ["x=Math.random()", "y=Math.random()", "z=Math.random()*50"]
)

fig = plt.figure()
ax = fig.subplots()
ax.set_xlim(0, 1)
ax.set_ylim(0, 1)
scat = ax.scatter([], [])  # Provide empty data initially
scatter_offsets = []  # Store separate arrays for offsets and sizes
scatter_sizes = []

# Note that Deephaven Express eliminates the complexity of update_fig and TableAnimation


def update_fig(data, update):
    # This assumes that table is always increasing. Otherwise need to look at other
    # properties in update for creates and removed items
    added = update.added()
    for i in range(0, len(added["x"])):
        # Append new data to the sources
        scatter_offsets.append([added["x"][i], added["y"][i]])
        scatter_sizes.append(added["z"][i])

    # Update the figure
    scat.set_offsets(scatter_offsets)
    scat.set_sizes(scatter_sizes)


ani = TableAnimation(fig, tt, update_fig)
```

![The above real-time scatter plot](../../assets/how-to/real_time_scatter.gif)

#### Multiple series

It's possible to have multiple kinds of series in the same figure. Here is an example of a line and a scatter plot:

```python skip-test
import matplotlib.pyplot as plt
from deephaven import time_table
from deephaven.plugin.matplotlib import TableAnimation

tt = time_table("PT1S").update(
    ["x=i", "y=Math.sin(x)", "z=Math.cos(x)", "r=Math.random()", "s=Math.random()*100"]
)

fig = plt.figure()
ax = fig.subplots()
(line1,) = ax.plot([], [])
(line2,) = ax.plot([], [])
scat = ax.scatter([], [])
scatter_offsets = []
scatter_sizes = []

# Note that Deephaven Express eliminates the complexity of update_fig and TableAnimation


def update_fig(data, update):
    line1.set_data([data["x"], data["y"]])
    line2.set_data([data["x"], data["z"]])
    added = update.added()
    for i in range(0, len(added["x"])):
        scatter_offsets.append([added["x"][i], added["r"][i]])
        scatter_sizes.append(added["s"][i])
    scat.set_offsets(scatter_offsets)
    scat.set_sizes(scatter_sizes)
    ax.relim()
    ax.autoscale_view(True, True, True)


ani = TableAnimation(fig, tt, update_fig)
```

![Multiple series on the same plot](../../assets/how-to/real_time_multiple_series.gif)

## Seaborn examples

### Static plotting

> [!CAUTION]
> All examples in this guide use [Matplotlib's explicit interface](https://matplotlib.org/stable/users/explain/figure/api_interfaces.html#api-interfaces). Users should do the same, especially in examples where there are multiple plots that source data from ticking tables.

Here is the basic usage of Seaborn to show one figure:

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

![The above plots created with Seaborn](../../assets/how-to/seaborn.png)

### Real-time plotting with TableAnimation

Seaborn is designed to plot data from pandas DataFrames. Unlike Matplotlib, we will convert table data to to DataFrames in the `update_fig` function.

#### Line plot

```python skip-test
from deephaven.plugin.matplotlib import TableAnimation
from deephaven import pandas as dhpd
from deephaven import time_table

import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd

# Create a ticking table with the cosine function
tt = time_table("PT1S").update(["X = 0.2 * i", "Y = Math.cos(X)"])

fig, ax = plt.subplots()  # Create a new figure

# Note that Deephaven Express eliminates the complexity of update_fig and TableAnimation


# This function updates a figure
def update_fig(data, update):
    # Clear the axes (don't draw over old lines)
    ax.clear()
    # Convert the X and Y columns in `tt` to a DataFrame
    df = dhpd.to_pandas(tt.view(["X", "Y"]))
    # Draw the line plot, pass `ax` to use explicit interface, which is strongly recommended
    sns.lineplot(df, x="X", y="Y", ax=ax)


# Create our animation. It will listen for updates on `tt` and call `update_fig` whenever there is an update
line_plot_ani = TableAnimation(fig, tt, update_fig)
```

![A real-time line plot created with Seaborn](../../assets/how-to/seaborn_line.gif)

#### Bar plot

```python skip-test
from deephaven.plugin.matplotlib import TableAnimation
from deephaven import pandas as dhpd
from deephaven import SortDirection
from deephaven import time_table

import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd

top_n = 5

# Create a ticking table with the linear function y = x
tt = time_table("PT1S").update(["X = i", "Y = i"])
# Sort `tt` by the top 5 values in the Y column
tt_sorted = tt.sort(order_by=["Y"], order=[SortDirection.DESCENDING])
# Create a table with 5 largest values for the chart
tt_sorted_top = tt_sorted.head(top_n)

fig, ax = plt.subplots()

# Note that Deephaven Express eliminates the complexity of update_fig and TableAnimation


# This function updates a figure
def update_fig(data, update):
    # Clear the axes (don't draw over old bars)
    ax.clear()
    # Convert the table of top 5 values to a DataFrame
    df = dhpd.to_pandas(tt_sorted_top.view(["X", "Y"]))
    # Draw the barplot, pass `ax` to use explicit interface, which is strongly recommended
    sns.barplot(df, x="X", y="Y", ax=ax)


# Create our animation. It will listen for updates on `tt` and call `update_fig` whenever there is an update
bar_plot_ani = TableAnimation(fig, tt_sorted_top, update_fig)
```

![A real-time bar plot created with Seaborn](../../assets/how-to/seaborn_bar.gif)

#### Scatter plot

```python skip-test
from deephaven.plugin.matplotlib import TableAnimation
from deephaven import pandas as dhpd
from deephaven import SortDirection
from deephaven import time_table

import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd

# Create a ticking table with random x, y, and z values
tt = time_table("PT2S").update(
    ["X = Math.random()", "Y = Math.random()", "Z = Math.random()*50"]
)

fig, ax = plt.subplots()

# Note that Deephaven Express eliminates the complexity of update_fig and TableAnimation


# This function updates a figure
def update_fig(data, update):
    # Clear the axes as to not draw over anything
    ax.clear()
    # Convert the X, Y, and Z columns to a dataframe
    df = dhpd.to_pandas(tt.view(["X", "Y", "Z"])).astype("object")
    # Draw the scatter plot, pass `ax` to use explicit interface, which is strongly recommended
    sns.scatterplot(df, x="X", y="Y", size="Z", ax=ax)


# Create our animation. It will listen for updates on `tt` and call `update_fig` whenever there is an update
ani = TableAnimation(fig, tt, update_fig)
```

![The above real-time scatter plot created with Seaborn](../../assets/how-to/seaborn_scatter.gif)

#### Multiple series

```python skip-test
from deephaven.plugin.matplotlib import TableAnimation
from deephaven import pandas as dhpd
from deephaven import SortDirection
from deephaven import time_table

import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd

# Create a ticking table with sine, cosine, and random numbers
tt = time_table("PT1S").update(
    [
        "X = i",
        "Y = Math.sin(X)",
        "Z = Math.cos(X)",
        "R = Math.random()",
        "S = Math.random()*50",
    ]
)

fig, ax = plt.subplots()

# Note that Deephaven Express eliminates the complexity of update_fig and TableAnimation


# This function updates a figure
def update_fig(data, update):
    # Clear the figure so we don't draw over stuff
    ax.clear()
    # Convert the data columns to a Pandas DataFrame
    df = dhpd.to_pandas(tt.view(["X", "Y", "Z", "R", "S"])).astype("object")
    # Draw two line plots and a scatterplot
    sns.lineplot(df, x="X", y="Y", ax=ax)
    sns.lineplot(df, x="X", y="Z", ax=ax)
    sns.scatterplot(df, x="X", y="R", size="S", ax=ax)


# Create our animation. It will listen for updates on `tt` and call `update_fig` whenever there is an update
ani = TableAnimation(fig, tt, update_fig)
```

![A multi-series line plot created with Seaborn](../../assets/how-to/seaborn_multipleplots.gif)

## Related documentation

- [How to create plots with the legacy API](./api-plotting.md)
- [Install and use plugins](../install-use-plugins.md)
- [Arrays](../../reference/query-language/types/arrays.md)
