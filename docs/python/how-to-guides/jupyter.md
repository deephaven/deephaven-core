---
title: Use Deephaven in Jupyter
---

This guide shows you how to use Deephaven from [Jupyter](https://jupyter.org/). Jupyter notebooks are powerful tools for sharing data science workflows with others through easy-to-follow interactive cells.

You can use Deephaven directly from Python with [pip-installed Deephaven](../getting-started/pip-install.md) or the [Python client](/core/client-api/python/). For either option, Deephaven also provides [widgets](https://pypi.org/project/deephaven-ipywidgets/) that display live Deephaven tables and figures in your notebook. For a guided, end-to-end walkthrough of pip-installed Deephaven in Jupyter, see the [Jupyter quickstart](../getting-started/jupyter-quickstart.md).

## Set up Jupyter and Deephaven

You can run Jupyter as either [Jupyter Notebook](https://jupyter-notebook.readthedocs.io/en/latest/) or [JupyterLab](https://jupyterlab.readthedocs.io/en/latest/). Both work for Deephaven, so you can choose whichever suits your needs better. See Project Jupyter's [installation guide](https://jupyter.org/install) to get started with Jupyter.

How you install Deephaven for Jupyter depends on how you use Deephaven from Python:

- **[Pip-installed Deephaven](../getting-started/pip-install.md)** runs the Deephaven server and your queries inside your notebook's Python process. This option requires Java.
- **The [Python client](/core/client-api/python/)** (`pydeephaven`) connects over gRPC to a Deephaven server that is already running elsewhere, such as in [Docker](../getting-started/docker-install.md) or on a remote machine.

Pick the one that matches where your server runs. This guide covers both. Whichever you pick, install every package in this section into the same Python environment that runs Jupyter, so your notebook can import them.

### pip-installed Deephaven

Set up Deephaven in your Python environment. To get started, follow the instructions in the [Install guide for pip](../getting-started/pip-install.md).

To plot with [Deephaven Express](/core/plotly/docs/), also install `deephaven-plugin-plotly-express` in the same environment before you start the Deephaven server in your notebook:

```sh
pip3 install deephaven-plugin-plotly-express
```

### The Python client

Install Deephaven's [Python client](/core/client-api/python/) with pip:

```sh
pip3 install pydeephaven
```

You can also build it from source. For instructions, see the [`pydeephaven` page on PyPI](https://pypi.org/project/pydeephaven/).

### deephaven-ipywidgets

Both options use [`deephaven-ipywidgets`](https://pypi.org/project/deephaven-ipywidgets/), a Python package that displays Deephaven tables, figures, and other Deephaven objects in Jupyter as widgets. Without it, Jupyter shows only a text representation of a Deephaven table or figure, not an interactive view. Install it with pip:

```sh
pip3 install deephaven-ipywidgets
```

You can also install the widget package together with the Deephaven package for your option, in one step:

- For [pip-installed Deephaven](../getting-started/pip-install.md), `pip3 install "deephaven-ipywidgets[server]"` also installs `deephaven-server`.
- For the [Python client](/core/client-api/python/), `pip3 install "deephaven-ipywidgets[client]"` also installs `pydeephaven`.

### Launch Jupyter

If you use Jupyter Notebook, start it with:

```sh
jupyter notebook
```

If you use JupyterLab, start it with:

```sh
jupyter lab
```

From there, create a notebook.

## Display tables and figures

### pip-installed Deephaven

With [pip-installed Deephaven](../getting-started/pip-install.md), start a Deephaven server in the notebook first, then use [`deephaven-ipywidgets`](https://pypi.org/project/deephaven-ipywidgets/) to display Deephaven tables or figures (plots).

> [!CAUTION]
> You **must** create a `deephaven_server.Server` **before** importing the `deephaven` package or any module that imports it. If you import `deephaven` first, the import raises a `RuntimeError`.

<!--TODO: Confirm whether DeephavenWidget works with a PSK-authenticated embedded server. If it does, switch this example to PSK. If it doesn't, link the issue that tracks PSK support here. (deephaven-ipywidgets#38 tracks shared export tickets, not PSK.) -->

Run the following code in a cell before you work with tables or figures. It starts a Deephaven server on port `10000` with [anonymous authentication](./authentication/auth-anon.md):

```python skip-test
# Start the Deephaven server
from deephaven_server import Server

s = Server(
    port=10000,
    jvm_args=["-DAuthHandlers=io.deephaven.auth.AnonymousAuthenticationHandler"],
)
s.start()
```

> [!NOTE]
> [Anonymous authentication](./authentication/auth-anon.md) provides no application security.

You display tables and figures by wrapping them in a `DeephavenWidget` from the [`deephaven-ipywidgets`](https://pypi.org/project/deephaven-ipywidgets/) package.

For example, to create a ticking table with [`time_table`](../reference/table-operations/create/timeTable.md) and display it:

```python skip-test
# Create a table and display it
from deephaven import time_table
from deephaven_ipywidgets import DeephavenWidget

t = time_table("PT1S").update(["X = 0.1 * i", "Y = sin(X)"])
display(DeephavenWidget(t))
```

![A ticking Deephaven table displayed in a Jupyter notebook](../assets/how-to/jupyter/table.png)

The table ticks in the notebook just as it does in the [Deephaven IDE](../getting-started/quickstart.md#2-the-deephaven-ide): a new row appears every second.

You can also create a figure with [`plot_xy`](../reference/plot/plot.md) and display it:

```python skip-test
from deephaven.plot.figure import Figure

f = Figure().plot_xy(series_name="Figure", t=t, x="X", y="Y").show()
display(DeephavenWidget(f))
```

![A Deephaven line plot of the sine column displayed in a Jupyter notebook](../assets/how-to/jupyter/figure.png)

This example uses the [built-in plotting API](./plotting/api-plotting.md), which is no longer under active development. For most plotting, use [Deephaven Express](/core/plotly/docs/). Deephaven Express needs the `deephaven-plugin-plotly-express` package, [installed before you start the server](#pip-installed-deephaven). The [plotting overview](./plotting/overview.md) compares the options.

To display a [Deephaven Express](/core/plotly/docs/) figure, pass it to `DeephavenWidget` the same way:

```python skip-test
import deephaven.plot.express as dx

display(DeephavenWidget(dx.line(t, x="X", y="Y")))
```

The widget is an iframe that your browser loads from the Deephaven server. By default, `DeephavenWidget` uses `http://localhost:{port}`, where `{port}` is the port the embedded `Server` runs on. If your browser reaches the server at a different address, such as when Jupyter runs in a Docker container, set the `DEEPHAVEN_IPY_URL` environment variable to that address before you create a `DeephavenWidget`. For example, if the server is mapped to port `1234` on your host:

```python skip-test
import os

os.environ["DEEPHAVEN_IPY_URL"] = "http://localhost:1234"
```

### The Python client

Use the [Python client](/core/client-api/python/) to connect to a Deephaven server that is already running. For a local server on port `10000` with [anonymous authentication](./authentication/auth-anon.md), you can create a session without arguments:

```python skip-test
from pydeephaven import Session

session = Session()
```

If the server uses [pre-shared key authentication](./authentication/auth-psk.md), as a default [Docker server](../getting-started/docker-install.md) does, pass the key:

```python skip-test
from pydeephaven import Session

session = Session(
    auth_type="io.deephaven.authentication.psk.PskAuthenticationHandler",
    auth_token="YOUR_PASSWORD_HERE",
)
```

To connect to a remote server or a server that uses another type of authentication, see the [`Session`](/core/client-api/python/code/pydeephaven.html#pydeephaven.Session) Pydoc. For a full introduction to the client, see the [Python client quickstart](../getting-started/pyclient-quickstart.md).

To display a table you create through the session, pass it to `DeephavenWidget`. The widget handles the rest. The example below also calls [`bind_table`](/core/client-api/python/code/pydeephaven.html#pydeephaven.Session.bind_table), which is optional. It gives the table a name, `my_time_table`, that the [web IDE](../getting-started/quickstart.md#2-the-deephaven-ide) and other sessions can use. Leave that line out if you only want to display the table:

```python skip-test
from deephaven_ipywidgets import DeephavenWidget

my_time_table = session.time_table(period="PT1S").update(["X = 0.1 * i", "Y = sin(X)"])
session.bind_table(name="my_time_table", table=my_time_table)
display(DeephavenWidget(my_time_table))
```

The widget loads the table from `http://{host}:{port}`, using the session's host and port. If your browser reaches the server at a different address, set `DEEPHAVEN_IPY_URL` as described for [pip-installed Deephaven](#pip-installed-deephaven-1).

## Related documentation

- [Jupyter quickstart](../getting-started/jupyter-quickstart.md)
- [Python client quickstart](../getting-started/pyclient-quickstart.md)
- [Install guide for pip](../getting-started/pip-install.md)
- [Install and run with Docker](../getting-started/docker-install.md)
- [Anonymous authentication](./authentication/auth-anon.md)
- [Pre-shared key authentication](./authentication/auth-psk.md)
- [Built-in plotting API](./plotting/api-plotting.md)
- [Plotting overview](./plotting/overview.md)
- [Deephaven Express](/core/plotly/docs/)
- [`plot_xy`](../reference/plot/plot.md)
- [`time_table`](../reference/table-operations/create/timeTable.md)
- [`update`](../reference/table-operations/select/update.md)
- [`Session` Pydoc](/core/client-api/python/code/pydeephaven.html#pydeephaven.Session)
- [`bind_table` Pydoc](/core/client-api/python/code/pydeephaven.html#pydeephaven.Session.bind_table)
- [`deephaven-ipywidgets` on PyPI](https://pypi.org/project/deephaven-ipywidgets/)
