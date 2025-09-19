---
title: Use Deephaven in Jupyter
sidebar_label: Jupyter
---

This guide will show you how to use Deephaven from [Jupyter](https://jupyter.org/). Jupyter notebooks are powerful tools that simplify the process of sharing data science workflows to others through easy-to-follow interactive cells.

Deephaven can be used directly from Python via [pip-installed Deephaven](../getting-started/pip-install.md) and the [Python client](/core/client-api/python/). It also has widgets built for working in [Jupyter](https://jupyter.org/). You can use these widgets to view Deephaven tables and figures.

## Installation

The installation process for Deephaven from Jupyter will depend on whether or not you use the server-side API (pip-installed Deephaven) or the Python client. Both are covered below.

Jupyter can be used either through its [Jupyter Notebooks](https://jupyter-notebook.readthedocs.io/en/latest/) or the more modern [JupyterLab](https://jupyterlab.readthedocs.io/en/latest/). Both work for Deephaven, so you can choose whichever suits your needs better. See Project Jupyter's [installation guide](https://jupyter.org/install) to get started with Jupyter.

### pip-installed Deephaven

Set up Deephaven in your Python environment. To get started, follow the instructions in the [Install guide for pip](../getting-started/pip-install.md) tutorial.

### The Python client

Deephaven's Python client can be installed through pip:

```sh
pip3 install pydeephaven
```

Or by building from source. The instructions for doing so can be found on [PyPi](https://pypi.org/project/pydeephaven/).

### deephaven-ipywidgets

Deephaven offers another Python package, [deephaven-ipywidgets](https://pypi.org/project/deephaven-ipywidgets/), which allows Deephaven tables and figures to be displayed in Jupyter through widgets. Without this package, tables and figures are not rendered. It is installed with pip:

```sh
pip3 install deephaven-ipywidgets
```

### Launch Jupyter

If you chose the basic Jupyter Notebook experience, start Jupyter with:

```sh
jupyter notebook
```

If you chose JupyterLab, start it with:

```sh
jupyter lab
```

From there, create a notebook.

## Usage

### pip-installed Deephaven

You can use `deephaven-ipywidgets` to display Deephaven tables or figures (plots). Before displaying either, you need to launch the Deephaven server. Add the following code in a cell to run before working with tables or figures:

```python skip-test
# Start up the Deephaven Server
from deephaven_server import Server

s = Server(port=8080)
s.start()
```

Next, wrap any tables or figures you want to display in a `DeephavenWidget`. For example, to create and display a table:

```python skip-test
# Create a table and display it
from deephaven import time_table
from deephaven_ipywidgets import DeephavenWidget

t = time_table("PT1S").update(["x=i", "y=Math.sin(i)"])
display(DeephavenWidget(t))
```

![Table](../assets/how-to/jupyter/table.png)

You can also create and display a figure:

```python skip-test
from deephaven.plot.figure import Figure

f = Figure().plot_xy(series_name="Figure", t=t, x="x", y="y").show()
display(DeephavenWidget(f))
```

![Figure](../assets/how-to/jupyter/figure.png)

By default, the Deephaven server is located at `http://localhost:{port}`, where `{port}` is the port set in the Deephaven server creation call. If the server is not there, such as when running Jupyter Notebook in a Docker container, modify the `DEEPHAVEN_IPY_URL` environmental variable to the correct URL before creating a `DeephavenWidget`.

```python skip-test
import os

os.environ["DEEPHAVEN_IPY_URL"] = "http://localhost:1234"
```

### The Python client

The Python client is used to connect to an already-running Deephaven server. For a server running locally on port 10000 with [anonymous authentication](./authentication/auth-anon.md), no input is required when creating a session.

```python skip-test
from pydeephaven import Session

session = Session()
```

For servers running remotely, or with authentication, see [Session](/core/client-api/python/code/pydeephaven.html#pydeephaven.Session).

Users can create tables through the client by executing commands and bind them to the server through the session.

```python skip-test
from deephaven_ipywidgets import DeephavenWidget

my_timetable = session.time_table(period=1000000000).update(
    ["X = 0.1 * i", "Y = sin(X)"]
)
session.bind_table(name="my_time_table", table=my_timetable)
display(DeephavenWidget(my_timetable))
```

<!-- TODO: Link to Python client docs when they exist (not pydocs) -->

## Related documentation

- [Install guide for pip](../getting-started/pip-install.md)
- [How to create XY series plots](./plotting/api-plotting.md#xy-series)
