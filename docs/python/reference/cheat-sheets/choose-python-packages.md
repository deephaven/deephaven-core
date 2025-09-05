---
title: Choose the right Deephaven Python packages to install
sidebar_label: Choose Python packages
---

This cheat sheet will help you choose the correct Deephaven Python packages to suit your needs.

## Packages

More than a dozen Deephaven packages are available for download via [`pypi.org`](https://pypi.org/search/?q=deephaven). Currently, available options are:

### `deephaven-core`

[`deephaven-core`](https://pypi.org/project/deephaven-core/) is the Deephaven Python Integration Package, which allows developers to access data, run queries, and execute Python scripts directly on Deephaven data servers to achieve maximum performance. Since this package is dependent on the Deephaven server, it comes preinstalled with Deephaven Docker images and is made available at runtime in the Python console in the Deephaven Web UI.

### `deephaven-server`

[`deephaven-server`](https://pypi.org/project/deephaven-server/) is Python-native way of launching Deephaven Community Core ([`deephaven-core`](https://www.github.com/deephaven/deephaven-core)), the open-source version of Deephaven Enterprise.

### `pydeephaven`

[`pydeephaven`](https://pypi.org/project/pydeephaven/) is Deephaven Python Client, a client API that allows Python applications to remotely access Deephaven data servers.

### `deephaven-ib`

[`deephaven-ib`](https://pypi.org/project/deephaven-ib/) is an [InteractiveBrokers](https://www.interactivebrokers.com/en/home.php) integration for Deephaven. See the package's [homepage](https://pypi.org/project/deephaven-ib/) for instructions on how to integrate with InteractiveBrokers.

### `deephaven-example-app`

[`deephaven-example-app`](https://pypi.org/project/deephaven-example-app/) is an example Deephaven application built using the [`deephaven-server`](https://pypi.org/project/deephaven-server/) package.

### `deephaven-plugin`

[`deephaven-plugin`](https://pypi.org/project/deephaven-plugin/) is the Deephaven Plugin interface. This package is currently in development.

### `deephaven-plugin-matplotlib`

[`deephaven-plugin-matplotlib`](https://pypi.org/project/deephaven-plugin-matplotlib/) is the Deephaven Plugin for matplotlib. It allows for opening matplotlib plots in a Deephaven environment.

### `deephaven-plugin-plotly`

[`deephaven-plugin-plotly`](https://pypi.org/project/deephaven-plugin-plotly/) is the Deephaven Plugin for Plotly. It allows for opening Plotly plots in a Deephaven environment.

### `deephaven-plugin-plotly-js`

[`deephaven-plugin-plotly-js`](https://pypi.org/project/deephaven-plugin-plotly-js/) is the Deephaven Plotly JavaScript plugin. It packages [`@deephaven/js-plugin-plotly`](https://www.npmjs.com/package/@deephaven/js-plugin-plotly) as a JavaScript plugin for [`deephaven-core`](https://github.com/deephaven/deephaven-core). This package is most useful in combination with [`deephaven-plugin-plotly`](https://github.com/deephaven/deephaven-plugin-plotly).

### `deephaven-plugin-plotly-express`

[`deephaven-plugin-plotly-express`](https://pypi.org/project/deephaven-plugin-plotly-express/) is a Deephaven plugin for charts that was built on top of Plotly Express.

### `deephaven-plugin-json`

[`deephaven-plugin-json`](https://pypi.org/project/deephaven-plugin-json/) is the Deephaven Plugin for JSON. This package is currently in development.

### `deephaven-jpy`

[`deephaven-jpy`](https://pypi.org/project/deephaven-jpy/) is a fork of [`jpy`](https://github.com/bcdev/jpy.git) maintained by Deephaven Data Labs.

### `deephaven-ipywidgets`

[`deephaven-ipywidgets`](https://pypi.org/project/deephaven-ipywidgets/) is the Deephaven Community IPython Widget Library.

### `streamlit-deephaven`

[`streamlit-deephaven`](https://pypi.org/project/streamlit-deephaven/) is a package to use Deephaven within Streamlit and display widgets.

### `deephaven`

[`deephaven`](https://pypi.org/project/deephaven/) is the package for Deephaven Enterprise.

## Related documentation

- [Install Python packages](../../how-to-guides/install-and-use-python-packages.md)
