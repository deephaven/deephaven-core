---
title: Configure JS plugins
sidebar_label: JS plugins
---

The Deephaven server supports custom JS plugins that extend the functionality of the [server](https://github.com/deephaven/deephaven-core) and [web client UI](https://github.com/deephaven/web-client-ui). `server:latest` ships with several plugins pre-bundled, including Matplotlib, Plotly Express, and the Deephaven UI. This guide shows how to install a plugin that isn't included in the base image, using [Plotly](https://plotly.com/) (basic) as the example.

## Quickstart

Installing a JS plugin involves up to 3 steps:

```docker title="Dockerfile"
FROM ghcr.io/deephaven/web-plugin-packager:latest as js-plugins
# 1. Package the NPM deephaven-js-plugin(s)
RUN ./pack-plugins.sh <plugins>

FROM ghcr.io/deephaven/server:latest
# 2. Install the Python package(s) if necessary (some plugins may be JS only)
RUN pip install --no-cache-dir <packages>
# 3. Copy the js-plugins/ directory
COPY --from=js-plugins js-plugins/ /opt/deephaven/config/js-plugins/
```

```bash
docker build -t my-deephaven-image .
docker run --rm -p 10000:10000 my-deephaven-image
```

The workflow above applies to the [Docker application](./docker-application.md), but the general requirements are the same when deploying the [production application](./configure-production-application.md). See [`pack-plugins.sh`](https://github.com/deephaven/deephaven-core/blob/main/docker/web-plugin-packager/src/main/docker/files/pack-plugins.sh) for more information on the JS plugin packaging logic.

## Configuration

The JS plugins are automatically sourced from the `<configDir>/js-plugins/` directory if present. In the case of the Docker example above, `/opt/deephaven/config/` is the configuration directory. The server looks for `manifest.json` in that directory to discover plugins — this file is generated automatically by `pack-plugins.sh`. See [configuration directory](./configure-production-application.md#deephaven-server-bootstrap-configuration) for information about the configuration directory for other setups.

The JS plugins directory can also be set explicitly through the [configuration property](./config-file.md) `deephaven.jsPlugins.resourceBase`.

## Examples

### Plotly

Here's an example installing the [Plotly](https://plotly.com/) JS plugin. This is the basic Plotly plugin, which enables rendering of raw `plotly.graph_objects.Figure` objects — distinct from `deephaven-plugin-plotly-express`, which is pre-bundled in `server:latest`:

```docker title="Dockerfile"
FROM ghcr.io/deephaven/web-plugin-packager:latest as js-plugins
RUN ./pack-plugins.sh @deephaven/js-plugin-plotly

FROM ghcr.io/deephaven/server:latest
RUN pip install --no-cache-dir deephaven-plugin-plotly
COPY --from=js-plugins js-plugins/ /opt/deephaven/config/js-plugins/
```

## Available JS plugins

Deephaven maintains a set of JS plugins in our GitHub repository [deephaven/deephaven-plugins](https://github.com/deephaven/deephaven-plugins).

These plugins have the keyword `deephaven-js-plugin` on [NPM](https://www.npmjs.com/), and can easily be [searched](https://www.npmjs.com/search?q=keywords%3Adeephaven-js-plugin).

Third-parties are welcome to develop their own JS plugins, or can reach out to suggest new plugins that Deephaven should develop.

## Related documentation

- [Install and use plugins](../install-use-plugins.md)
- [Create a JavaScript plugin](../create-js-plugins.md)
