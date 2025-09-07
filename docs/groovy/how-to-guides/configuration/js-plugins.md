---
title: Configure JS plugins
sidebar_label: JS plugins
---

The Deephaven server has the ability to work with custom JS plugins that extend the functionality of the [server](https://github.com/deephaven/deephaven-core) and [web client UI](https://github.com/deephaven/web-client-ui). This guide shows you how to install JS plugins and provides examples for two specific plugins, [Plotly](https://plotly.com/) and [Matplotlib](https://matplotlib.org/).

## Quickstart

Installing a JS plugin involves up to 3 steps:

```docker title="Dockerfile"
FROM ghcr.io/deephaven/web-plugin-packager:latest as js-plugins
# 1. Package the NPM deephaven-js-plugin(s)
RUN ./pack-plugins.sh <plugins>

FROM ghcr.io/deephaven/server:latest
# 2. Install the python js-plugin(s) if necessary (some plugins may be JS only)
RUN pip install --no-cache-dir <packages>
# 3. Copy the js-plugins/ directory
COPY --from=js-plugins js-plugins/ /opt/deephaven/config/js-plugins/
```

<></>

```bash
docker build -t my-deephaven-image .
docker run --rm -p 10000:10000 my-deephaven-image
```

The workflow above is shown in relationship to the [Docker application](./docker-application.md), but the general requirements are the same if one is deploying the [production application](./configure-production-application.md). See [pack-plugins.sh](https://github.com/deephaven/deephaven-core/blob/main/docker/web-plugin-packager/src/main/docker/files/pack-plugins.sh) for more information on the JS plugin packaging logic.

## Configuration

The JS plugins are automatically sourced from the `<configDir>/js-plugins/` directory if present. In the case of the Docker example above, `/opt/deephaven/config/` is the configuration directory. See [here](./configure-production-application.md#deephaven-server-bootstrap-configuration) for information about the configuration directory for other setups.

The JS plugins directory can also be set explicitly through the [configuration property](./config-file.md) `deephaven.jsPlugins.resourceBase`.

## Examples

### Plotly

Here's an example installing the [Plotly](https://plotly.com/) JS plugin:

```docker title="Dockerfile"
FROM ghcr.io/deephaven/web-plugin-packager:latest as js-plugins
RUN ./pack-plugins.sh @deephaven/js-plugin-plotly

FROM ghcr.io/deephaven/server:latest
RUN pip install --no-cache-dir deephaven-plugin-plotly
COPY --from=js-plugins js-plugins/ /opt/deephaven/config/js-plugins/
```

### Plotly + Matplotlib

Here's an example installing the [Plotly](https://plotly.com/) and [Matplotlib](https://matplotlib.org/) JS plugins:

```docker title="Dockerfile"
FROM ghcr.io/deephaven/web-plugin-packager:latest as js-plugins
RUN ./pack-plugins.sh @deephaven/js-plugin-plotly @deephaven/js-plugin-matplotlib

FROM ghcr.io/deephaven/server:latest
RUN pip install --no-cache-dir deephaven-plugin-plotly deephaven-plugin-matplotlib
COPY --from=js-plugins js-plugins/ /opt/deephaven/config/js-plugins/
```

## Available JS Plugins

Deephaven maintains a set of JS plugins in our GitHub repository [deephaven/deephaven-js-plugins](https://github.com/deephaven/deephaven-js-plugins).

These plugins have the keyword `deephaven-js-plugin` on [NPM](https://www.npmjs.com/), and can easily be [searched](https://www.npmjs.com/search?q=keywords%3Adeephaven-js-plugin).

Third-parties are welcome to develop their own JS plugins, or can reach out to suggest new plugins that Deephaven should develop.

## Related documentation

- [Install and use plugins](../install-use-plugins.md)
