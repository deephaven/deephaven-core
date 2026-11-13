---
title: Choose the right Deephaven Python packages to install
---

This cheat sheet describes the Python packages on [PyPI](https://pypi.org/search/?q=deephaven) that you use with Deephaven, when to install each one, and which legacy or unsupported packages a PyPI search also returns. Start with the table below, then see the entry for each package.

## Which package to install

| To...                                                                                                                                                   | Install                                                                                                                                                                                                                                                                                                                                 |
| ------------------------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Write queries in a Deephaven server that runs from a Python Docker image, such as [`ghcr.io/deephaven/server`](../../getting-started/docker-install.md) | Nothing. Every Deephaven Python image includes [`deephaven-core`](#deephaven-core). Only [`ghcr.io/deephaven/server`](../../getting-started/docker-install.md) also includes the [Deephaven Express](#deephaven-plugin-plotly-express), [`deephaven.ui`](#deephaven-plugin-ui), and [Matplotlib](#deephaven-plugin-matplotlib) plugins. |
| Write queries in a Deephaven server that runs from the [production application](../../getting-started/production-application.md)                        | [`deephaven-core[autocomplete]`](#deephaven-core), installed into the server's virtual environment                                                                                                                                                                                                                                      |
| Run a Deephaven server from Python, without Docker                                                                                                      | [`deephaven-server`](#deephaven-server)                                                                                                                                                                                                                                                                                                 |
| Connect to a running Deephaven server from another Python process                                                                                       | [`pydeephaven`](#pydeephaven)                                                                                                                                                                                                                                                                                                           |
| Subscribe to [ticking tables](../../conceptual/table-types.md#standard-streaming-tables) from a Python client                                           | [`pydeephaven-ticking`](#pydeephaven-ticking)                                                                                                                                                                                                                                                                                           |
| Plot Deephaven tables                                                                                                                                   | [`deephaven-plugin-plotly-express`](#deephaven-plugin-plotly-express) (preinstalled in [`ghcr.io/deephaven/server`](../../getting-started/docker-install.md))                                                                                                                                                                           |
| Build real-time dashboards                                                                                                                              | [`deephaven-plugin-ui`](#deephaven-plugin-ui) (preinstalled in [`ghcr.io/deephaven/server`](../../getting-started/docker-install.md))                                                                                                                                                                                                   |
| Display Matplotlib and Seaborn plots                                                                                                                    | [`deephaven-plugin-matplotlib`](#deephaven-plugin-matplotlib) (preinstalled in [`ghcr.io/deephaven/server`](../../getting-started/docker-install.md)). For Seaborn, install [`deephaven-plugin-matplotlib[seaborn]`](#deephaven-plugin-matplotlib).                                                                                     |
| Create TradingView Lightweight Charts                                                                                                                   | [`deephaven-plugin-tradingview-lightweight`](#deephaven-plugin-tradingview-lightweight)                                                                                                                                                                                                                                                 |
| Add color themes to the Deephaven Web UI                                                                                                                | [`deephaven-plugin-theme-pack`](#deephaven-plugin-theme-pack)                                                                                                                                                                                                                                                                           |
| Write a Deephaven server-side plugin                                                                                                                    | [`deephaven-plugin`](#deephaven-plugin)                                                                                                                                                                                                                                                                                                 |
| Use local Python workspaces from the [Deephaven VS Code extension](https://deephaven.io/vscode/docs/)                                                   | [`deephaven-plugin-python-remote-file-source`](#deephaven-plugin-python-remote-file-source)                                                                                                                                                                                                                                             |
| Use Deephaven in Jupyter                                                                                                                                | [`deephaven-ipywidgets[server]`](#deephaven-ipywidgets) to run the server in the notebook, or [`deephaven-ipywidgets[client]`](#deephaven-ipywidgets) to connect to a running server                                                                                                                                                    |
| Display Deephaven tables in a Streamlit app                                                                                                             | [`streamlit-deephaven`](#streamlit-deephaven)                                                                                                                                                                                                                                                                                           |
| Trade through Interactive Brokers                                                                                                                       | [`deephaven-ib`](#deephaven-ib)                                                                                                                                                                                                                                                                                                         |
| Connect AI tools to Deephaven                                                                                                                           | [`deephaven-mcp`](#deephaven-mcp)                                                                                                                                                                                                                                                                                                       |

## Server and engine

### `deephaven-core`

[`deephaven-core`](https://pypi.org/project/deephaven-core/) is the Deephaven Python Integration Package. It lets Python code access data, run queries, and execute scripts directly inside a Deephaven server, next to the data, on static and real-time tables alike. Python code imports it as [`deephaven`](../../how-to-guides/deephaven-python-package.md). A legacy Enterprise package on PyPI, [`deephaven`](#deephaven), also installs a module called `deephaven`, but that module is unrelated.

`deephaven-core` runs only inside a Deephaven server, and the Deephaven Web UI's Python console has it available at runtime. The Deephaven Python Docker images, such as [`ghcr.io/deephaven/server`](../../getting-started/docker-install.md), include it. For a server run from the [production application](../../getting-started/production-application.md), install it into the server's virtual environment.

When you install it yourself, use `deephaven-core[autocomplete]`. The `[autocomplete]` suffix names an extra. An extra is an optional set of dependencies that `pip` installs only when you name the extra in brackets after the package name. This extra installs the libraries that provide code completion in the Deephaven Web UI.

### `deephaven-server`

[`deephaven-server`](https://pypi.org/project/deephaven-server/) is a Python-native way to launch Deephaven Community Core. It embeds the server in a Python process, so you can install it with `pip` and run it without Docker. Installing it also installs [`deephaven-core[autocomplete]`](#deephaven-core) and [`jpy`](#jpy). You need Java 17 or later. See [Install and run Deephaven with pip](../../getting-started/pip-install.md) for the full prerequisites.

### `jpy`

[`jpy`](https://pypi.org/project/jpy/) is the bidirectional Python-Java bridge that [`deephaven-core`](#deephaven-core) and [`deephaven-server`](#deephaven-server) depend on. The [jpy consortium](https://github.com/jpy-consortium/jpy) maintains it, and installing either package installs `jpy` as a dependency. See [Use jpy](../../how-to-guides/use-jpy.md).

## Clients

### `pydeephaven`

[`pydeephaven`](https://pypi.org/project/pydeephaven/) is the Deephaven Python Client, a client API that lets Python applications access a Deephaven server remotely. See the [Python client quickstart](../../getting-started/pyclient-quickstart.md).

### `pydeephaven-ticking`

[`pydeephaven-ticking`](https://pypi.org/project/pydeephaven-ticking/) extends [`pydeephaven`](#pydeephaven) so a client can subscribe to [ticking tables](../../conceptual/table-types.md#standard-streaming-tables) and receive the added, removed, and modified rows of each update. It installs [`pydeephaven`](#pydeephaven) as a dependency. PyPI has prebuilt wheels for Linux x86_64 only. For other platforms, see the [version support matrix](../version-matrix.md#client-libraries).

## Plugins and plugin development

Install a plugin into the Python environment the server runs in, then restart the server, because the server loads plugins only when it starts. In Docker, add the `pip install` line to a custom image rather than running it in a container. See [Install and use plugins](../../how-to-guides/install-use-plugins.md#install-with-pip).

### `deephaven-plugin-plotly-express`

[`deephaven-plugin-plotly-express`](https://pypi.org/project/deephaven-plugin-plotly-express/) is [Deephaven Express](/core/plotly/docs/), a Deephaven charting plugin built on [Plotly Express](https://plotly.com/python/plotly-express/). It is the recommended way to plot Deephaven tables. The [`ghcr.io/deephaven/server`](../../getting-started/docker-install.md) Docker image includes it.

### `deephaven-plugin-ui`

[`deephaven-plugin-ui`](https://pypi.org/project/deephaven-plugin-ui/) is the plugin for building real-time dashboards with [`deephaven.ui`](/core/ui/docs/). The [`ghcr.io/deephaven/server`](../../getting-started/docker-install.md) Docker image includes it.

### `deephaven-plugin-matplotlib`

[`deephaven-plugin-matplotlib`](https://pypi.org/project/deephaven-plugin-matplotlib/) is the Deephaven plugin for Matplotlib. It displays Matplotlib and Seaborn plots in Deephaven. The [`ghcr.io/deephaven/server`](../../getting-started/docker-install.md) Docker image includes it but not Seaborn. To install Seaborn as well, install the `deephaven-plugin-matplotlib[seaborn]` [extra](#deephaven-core). See [Matplotlib and Seaborn](../../how-to-guides/plotting/matplot-seaborn.md).

### `deephaven-plugin-tradingview-lightweight`

[`deephaven-plugin-tradingview-lightweight`](https://pypi.org/project/deephaven-plugin-tradingview-lightweight/) is a Deephaven plugin for creating [TradingView Lightweight Charts](https://www.tradingview.com/lightweight-charts/) from Python. The [`ghcr.io/deephaven/server`](../../getting-started/docker-install.md) Docker image does not include it, so install it as described at the top of this section.

### `deephaven-plugin-theme-pack`

[`deephaven-plugin-theme-pack`](https://pypi.org/project/deephaven-plugin-theme-pack/) adds color themes to the Deephaven Web UI. The [`ghcr.io/deephaven/server`](../../getting-started/docker-install.md) Docker image does not include it, so install it as described at the top of this section. See [Install and use plugins](../../how-to-guides/install-use-plugins.md) and [Create custom themes](../../how-to-guides/custom-themes.md).

### `deephaven-plugin`

[`deephaven-plugin`](https://pypi.org/project/deephaven-plugin/) is the Python interface for writing Deephaven server-side plugins. [`deephaven-core`](#deephaven-core) depends on it, so installing [`deephaven-core`](#deephaven-core) also installs `deephaven-plugin`. Install it directly only when developing a plugin. See [Create your own plugin](../../how-to-guides/create-plugins.md).

### `deephaven-plugin-python-remote-file-source`

[`deephaven-plugin-python-remote-file-source`](https://pypi.org/project/deephaven-plugin-python-remote-file-source/) is a [bidirectional plugin](../../how-to-guides/create-plugins.md) that lets Python code import modules from a remote file source. The [Deephaven VS Code extension](https://deephaven.io/vscode/docs/) uses it to enable local Python workspaces. The [`ghcr.io/deephaven/server`](../../getting-started/docker-install.md) Docker image does not include it, so install it as described at the top of this section.

### `deephaven-plugin-utilities`

[`deephaven-plugin-utilities`](https://pypi.org/project/deephaven-plugin-utilities/) is a helper library that Deephaven plugins use at runtime. It is not a plugin, and you do not install it directly. Installing one of the plugins above, such as [`deephaven-plugin-plotly-express`](#deephaven-plugin-plotly-express) or [`deephaven-plugin-ui`](#deephaven-plugin-ui), installs it as a dependency.

### `deephaven-plugin-packaging`

[`deephaven-plugin-packaging`](https://pypi.org/project/deephaven-plugin-packaging/) is a helper library that the Deephaven plugin build process uses to create plugin wheels. It is not a plugin, and you do not install it directly. Installing a plugin from a prebuilt wheel does not install it.

## Notebooks, apps, and integrations

### `deephaven-ipywidgets`

[`deephaven-ipywidgets`](https://pypi.org/project/deephaven-ipywidgets/) displays Deephaven tables, figures, and other Deephaven objects as interactive widgets in Jupyter notebooks. The `deephaven-ipywidgets[server]` [extra](#deephaven-core) also installs [`deephaven-server`](#deephaven-server), and the `deephaven-ipywidgets[client]` extra also installs [`pydeephaven`](#pydeephaven). See [Use Deephaven in Jupyter](../../how-to-guides/jupyter.md).

### `streamlit-deephaven`

[`streamlit-deephaven`](https://pypi.org/project/streamlit-deephaven/) displays Deephaven tables and other Deephaven widgets in a [Streamlit](https://streamlit.io/) app. It runs the Deephaven server through [`deephaven-server`](#deephaven-server), which it installs as a dependency.

### `deephaven-ib`

[`deephaven-ib`](https://pypi.org/project/deephaven-ib/) is an [Interactive Brokers](https://www.interactivebrokers.com/en/home.php) integration for Deephaven. It runs on [`deephaven-server`](#deephaven-server), which it installs as a dependency. See the [`deephaven-ib` README](https://github.com/deephaven-examples/deephaven-ib) for setup and usage.

### `deephaven-mcp`

[`deephaven-mcp`](https://pypi.org/project/deephaven-mcp/) provides Model Context Protocol (MCP) servers that connect AI tools to Deephaven. See [Deephaven MCP](../../how-to-guides/mcp.md).

## Legacy and unsupported packages

These packages appear in a PyPI search for Deephaven, but Deephaven Community Core does not use them. A PyPI search also returns experimental plugins published by individual developers, which Deephaven does not document or support.

### `deephaven-jpy`

[`deephaven-jpy`](https://pypi.org/project/deephaven-jpy/) is a legacy Deephaven fork of [`jpy`](#jpy), last released in 2022. [`deephaven-core`](#deephaven-core) and [`deephaven-server`](#deephaven-server) depend on [`jpy`](#jpy) instead and do not use `deephaven-jpy`.

### `deephaven-plugin-plotly`

[`deephaven-plugin-plotly`](https://pypi.org/project/deephaven-plugin-plotly/) is a legacy Deephaven plugin for displaying Plotly figures. Deephaven no longer supports it. To display a Plotly figure, use [`deephaven-plugin-plotly-express`](#deephaven-plugin-plotly-express), which also renders raw Plotly figures.

### `deephaven-plugin-plotly-js`

[`deephaven-plugin-plotly-js`](https://pypi.org/project/deephaven-plugin-plotly-js/) packaged the JavaScript side of the legacy [`deephaven-plugin-plotly`](#deephaven-plugin-plotly). PyPI has only development pre-releases of it. Use [`deephaven-plugin-plotly-express`](#deephaven-plugin-plotly-express) instead.

### `deephaven-plugin-json`

[`deephaven-plugin-json`](https://pypi.org/project/deephaven-plugin-json/) is a Deephaven plugin for JSON that is available on PyPI only as development pre-releases. It has no stable release to install.

### `deephaven-example-app`

[`deephaven-example-app`](https://pypi.org/project/deephaven-example-app/) is an example Deephaven application built on [`deephaven-server`](#deephaven-server) that ingests a real-time data feed, transforms it, and displays it in the Web UI. It was last released in 2022.

### `deephaven`

[`deephaven`](https://pypi.org/project/deephaven/) is a legacy Python package for Deephaven Enterprise, last released in January 2023. It is not part of Deephaven Community Core. It installs a module that is also imported as `deephaven` but is unrelated to the [`deephaven`](../../how-to-guides/deephaven-python-package.md) module that Community queries use, which comes from [`deephaven-core`](#deephaven-core). For Deephaven Enterprise, use the [`deephaven-coreplus-client`](https://pypi.org/project/deephaven-coreplus-client/) Python client.

## Related documentation

- [Table types](../../conceptual/table-types.md)
- [Install and run with Docker](../../getting-started/docker-install.md)
- [Install and run Deephaven with pip](../../getting-started/pip-install.md)
- [Install and run the Deephaven production application](../../getting-started/production-application.md)
- [Jupyter quickstart](../../getting-started/jupyter-quickstart.md)
- [Python client quickstart](../../getting-started/pyclient-quickstart.md)
- [Create custom themes](../../how-to-guides/custom-themes.md)
- [Create your own plugin](../../how-to-guides/create-plugins.md)
- [Deephaven Express](/core/plotly/docs/)
- [Deephaven MCP](../../how-to-guides/mcp.md)
- [Deephaven's Python package](../../how-to-guides/deephaven-python-package.md)
- [`deephaven.ui`](/core/ui/docs/)
- [Install and use plugins](../../how-to-guides/install-use-plugins.md)
- [Install Python packages](../../how-to-guides/install-and-use-python-packages.md)
- [Matplotlib and Seaborn](../../how-to-guides/plotting/matplot-seaborn.md)
- [Use Deephaven in Jupyter](../../how-to-guides/jupyter.md)
- [Use jpy](../../how-to-guides/use-jpy.md)
- [Version support matrix](../version-matrix.md)
