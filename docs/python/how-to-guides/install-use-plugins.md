---
title: Install and use plugins
---

This guide covers how to install and use pre-built plugins in Deephaven. A plugin extends the functionality of Deephaven, much as a Python package extends Python. Some plugins are distributed as Python packages, but not every Python package is a plugin. Plugins in Deephaven can extend the functionality of a running server, UI, client API, or all of them.

Deephaven offers several pre-built plugins that extend the platform's functionality, and anyone using Deephaven Community Core can install them. For the plugins you can install, see [Available plugins](#available-plugins).

> [!NOTE]
> To install Linux system packages rather than plugins, see [Install packages](./install-packages.md). To install Python packages, see [Install and use Python packages](./install-and-use-python-packages.md).
>
> To have _complete control_ of the build process, see [Build and launch Deephaven from source code](../getting-started/launch-build.md).

For information on building your own plugins, see [Create your own plugin](./create-plugins.md).

## Types of plugins

Deephaven plugins come in four types:

- **Server-side plugins** extend the Deephaven server. For example, plotting plugins let you plot with [Deephaven Express](/core/plotly/docs/) or with [Matplotlib and Seaborn](./plotting/matplot-seaborn.md). Authentication plugins add authentication methods such as [mutual TLS (mTLS)](./authentication/auth-mtls.md).
- **UI plugins**, also called JavaScript (JS) plugins, extend the Deephaven web IDE. They are JavaScript packages that the server loads and serves to the browser. For example, the [theme pack](https://github.com/deephaven/deephaven-plugins/tree/main/plugins/theme-pack) adds IDE color themes.
- **Client-side plugins** extend Deephaven's client APIs. For example, a plugin can extend the [Python client API](../getting-started/pyclient-quickstart.md) so the client can manage arbitrary Python objects on the server, or so it can interact with the server using a different serialization format.
- **Bidirectional plugins** keep a message stream open between a server-side object and a client so that the two can exchange messages. Many also ship a client-side plugin. For example, the [remote file source plugin](#remote-file-sourcing) lets Python code on the server import modules from files that a client, such as the [Deephaven VS Code extension](https://deephaven.io/vscode/docs/), provides. See [Bidirectional plugin examples](#bidirectional-plugin-examples).

## Install a plugin

> [!NOTE]
> Authentication plugins need additional configuration. See [Authentication](#authentication) for the guide to each one.

How you install a plugin depends on how it is packaged:

- **Python package** (with or without JS bundled in its wheel): install it with `pip`. See [Install with pip](#install-with-pip).
- **npm package** (a JS component that isn't in a wheel): package it with `pack-plugins.sh` and copy the result to the directory where the server looks for JS plugins. In Docker, build a custom image on the [pre-built Deephaven image](../getting-started/docker-install.md), as described in [Extend Docker images for JS plugins](#extend-docker-images-for-js-plugins). For other installations, see [Configure JS plugins](./configuration/js-plugins.md).

### Install with pip

You can install some plugins with `pip`. Install them into the Python environment the server runs in. The server loads plugins only when it starts, so restart the server after you install a plugin.

In Docker, don't run `pip install` inside a running container. Add the `pip install` line to a custom image instead. The server then loads the plugin when the container starts, and the plugin survives container restarts. For example, the following Dockerfile adds the [theme pack](https://pypi.org/project/deephaven-plugin-theme-pack/):

```docker title="Dockerfile"
FROM ghcr.io/deephaven/server:latest
RUN pip install --no-cache-dir deephaven-plugin-theme-pack
```

Build and run the image the same way as in [Example: install an npm-only JS plugin](#example-install-an-npm-only-js-plugin).

The [user interface](#user-interface) and [remote file sourcing](#remote-file-sourcing) plugins install with `pip`. The `ghcr.io/deephaven/server` Docker image already includes [`deephaven-plugin-ui`](https://pypi.org/project/deephaven-plugin-ui/), [`deephaven-plugin-plotly-express`](https://pypi.org/project/deephaven-plugin-plotly-express/), and [`deephaven-plugin-matplotlib`](https://pypi.org/project/deephaven-plugin-matplotlib/), so you don't need to install those there.

For more information on installing Python packages in Deephaven, see [Install and use Python packages](./install-and-use-python-packages.md).

### Extend Docker images for JS plugins

Plugins whose JS component is published to npm, rather than bundled in the Python `.whl` file, need more setup than `pip install`. To install one in Docker, build a new Docker image based on Deephaven that adds the plugin.

Each plugin's npm package name is the `name` field of `src/js/package.json` in its folder of the [plugins repository](#the-plugins-repository). You can also [search npm for the `deephaven-js-plugin` keyword](https://www.npmjs.com/search?q=keywords%3Adeephaven-js-plugin).

The following Dockerfile is the general template for a plugin with both JavaScript and Python components:

```docker title="Dockerfile"
FROM ghcr.io/deephaven/web-plugin-packager:latest AS js-plugins
# 1. Package the npm deephaven-js-plugin(s)
RUN ./pack-plugins.sh <plugins>

FROM ghcr.io/deephaven/server:latest
# 2. Install the Python package(s) if necessary (some plugins may be JS only)
RUN pip install --no-cache-dir <packages>
# 3. Copy the js-plugins/ directory
COPY --from=js-plugins js-plugins/ /opt/deephaven/config/js-plugins/
```

To fill in the template:

- Replace `<plugins>` with one or more npm package names, separated by spaces. To pin a version, append `@<version>` to a name.
- Replace `<packages>` with the plugin's Python package names. For a JS-only plugin, omit the `pip install` line.

For how the server discovers JS plugins and how to change the plugin directory, see [Configure JS plugins](./configuration/js-plugins.md).

#### Example: install an npm-only JS plugin

The dashboard object viewer plugin adds an IDE panel that displays server objects other than tables and figures. Its JS is published only to npm, as `@deephaven/js-plugin-dashboard-object-viewer`, so `pip` can't install it.

1. Create a directory for your deployment and save the following as `Dockerfile` in it:

   ```docker title="Dockerfile"
   FROM ghcr.io/deephaven/web-plugin-packager:latest AS js-plugins
   RUN ./pack-plugins.sh @deephaven/js-plugin-dashboard-object-viewer

   FROM ghcr.io/deephaven/server:latest
   COPY --from=js-plugins js-plugins/ /opt/deephaven/config/js-plugins/
   ```

2. Build the image and run it:

   ```bash
   docker build -t my-deephaven-image .
   docker run --rm -p 10000:10000 my-deephaven-image
   ```

   For more `docker run` options, see [Configure the Deephaven Docker application](./configuration/docker-application.md).

   If you use [Docker Compose](https://docs.docker.com/compose/), save the following as `docker-compose.yml` in the same directory. It builds the image from your Dockerfile rather than pulling one from the registry:

   ```yaml title="docker-compose.yml"
   services:
     deephaven:
       build:
         context: .
       ports:
         - "10000:10000"
   ```

   Then build and run with a single command, instead of the two `docker` commands:

   ```bash
   docker compose up --build
   ```

3. Confirm that the server loaded the plugin. The server lists its JS plugins at `http://localhost:10000/js-plugins/manifest.json`:

   ```bash
   curl http://localhost:10000/js-plugins/manifest.json
   ```

   The output lists each JS plugin that the server loaded, by its npm package name. A plugin you packaged appears without any `@<version>` suffix you passed to `pack-plugins.sh`. JS plugins installed with `pip` appear too.

   Here, the output includes `@deephaven/js-plugin-dashboard-object-viewer`. It also includes the JS plugins for [Deephaven Express](/core/plotly/docs/), [Matplotlib](./plotting/matplot-seaborn.md), and [deephaven.ui](/core/ui/docs/), which the `server` image installs with `pip`.

## Use a plugin

After installation, each plugin's own documentation covers its API. For example, see [deephaven.ui](/core/ui/docs/), [Deephaven Express](/core/plotly/docs/), and [Matplotlib and Seaborn](./plotting/matplot-seaborn.md).

To use a client-side or bidirectional plugin from the [Python client](../getting-started/pyclient-quickstart.md):

1. Get the plugin's object on the server by its variable name from [`Session.exportable_objects`](/core/client-api/python/code/pydeephaven.session.html#pydeephaven.session.Session.exportable_objects).
2. Wrap it in a [`PluginClient`](/core/client-api/python/code/pydeephaven.experimental.plugin_client.html#pydeephaven.experimental.plugin_client.PluginClient) with [`Session.plugin_client`](/core/client-api/python/code/pydeephaven.session.html#pydeephaven.session.Session.plugin_client).

For a worked example, see [Client-side testing](./create-plugins.md#client-side-testing).

## Available plugins

### The plugins repository

Deephaven hosts a [plugins repository](https://github.com/deephaven/deephaven-plugins) that contains many of the official plugins offered. It's a good place to find more information about the plugins and view their source code.

> [!NOTE]
> Some plugins are not in the plugins repository but are still available for use. For instance, some authentication plugins are JAR files only available on Maven Central.

Two folders in the plugins repository are of particular interest:

- [plugins](https://github.com/deephaven/deephaven-plugins/tree/main/plugins): Contains the implementation of the plugins. This is where you can find each plugin's source code and additional documentation.
- [templates](https://github.com/deephaven/deephaven-plugins/tree/main/templates): Contains templates for creating new plugins. This is a great starting point if you want to [create your own plugin](./create-plugins.md).

The following sections group some of the available plugins by functionality.

### User interface

You can install each of the following user interface plugins with `pip`. The `ghcr.io/deephaven/server` image already includes the first three.

- [`deephaven-plugin-ui`](https://pypi.org/project/deephaven-plugin-ui/): A plugin for building real-time dashboards with [deephaven.ui](/core/ui/docs/).
- [`deephaven-plugin-plotly-express`](https://pypi.org/project/deephaven-plugin-plotly-express/): A plugin, [Deephaven Express](/core/plotly/docs/), that makes [Plotly Express](https://plotly.com/python/plotly-express/) compatible with Deephaven tables.
- [`deephaven-plugin-matplotlib`](https://pypi.org/project/deephaven-plugin-matplotlib/): A plugin that makes [Matplotlib](https://matplotlib.org/) and [Seaborn](https://seaborn.pydata.org/) compatible with Deephaven tables. See [Matplotlib and Seaborn](./plotting/matplot-seaborn.md).
- [`deephaven-plugin-theme-pack`](https://pypi.org/project/deephaven-plugin-theme-pack/): A plugin that adds UI themes such as Dracula, Night Owl, and Solarized Dark to the Deephaven IDE.

### Remote file sourcing

You can install this plugin with `pip`:

- [`deephaven-plugin-python-remote-file-source`](https://pypi.org/project/deephaven-plugin-python-remote-file-source/): A [bidirectional plugin](./create-plugins.md) that lets Python code import modules from a remote file source. Use it with the [Deephaven VS Code extension](https://deephaven.io/vscode/docs/) to enable local Python workspaces.

### Authentication

Authentication plugins have a more complex installation process than other plugins. See the following guides for details:

- [Keycloak](./authentication/auth-keycloak.md): A plugin that enables the use of [Keycloak](https://www.keycloak.org/) and [OpenID Connect (OIDC)](https://openid.net/developers/how-connect-works/) for authentication.
- [mTLS](./authentication/auth-mtls.md): A plugin that enables [mutual TLS (mTLS)](https://www.cloudflare.com/learning/access-management/what-is-mutual-tls/) authentication.
- [Username/password](./authentication/auth-uname-pw.md): A plugin that enables username/password authentication.

### Bidirectional plugin examples

[Bidirectional plugins](./create-plugins.md) let you define custom remote procedure call (RPC) methods that clients call to interact with objects on a running server and get objects back. See the following examples:

- [Pickle RPC plugin](https://github.com/deephaven-examples/plugin-python-rpc-pickle): A plugin to remotely execute methods on a Deephaven server. Its client code imports `pydeephaven.experimental.server_object`, which `pydeephaven` removed in version 0.37.0, so it needs updating before it runs with current clients. It uses Python's `pickle` module, which is not secure. Read its README before you adapt it.
- [Example bidirectional plugin](https://github.com/deephaven-examples/plugin-bidirectional-example): A minimal bidirectional plugin and a client that uses it. It is an earlier version of the example in [Create your own plugin](./create-plugins.md). Its client code imports `pydeephaven.experimental.server_object`, which `pydeephaven` removed in version 0.37.0, so follow the code in that guide instead.

## Related documentation

- [Build and launch Deephaven from source code](../getting-started/launch-build.md)
- [Configure JS plugins](./configuration/js-plugins.md)
- [Configure Keycloak for authentication](./authentication/auth-keycloak.md)
- [Configure mTLS for Deephaven](./authentication/auth-mtls.md)
- [Configure the Deephaven Docker application](./configuration/docker-application.md)
- [Configure username/password authentication](./authentication/auth-uname-pw.md)
- [Create your own plugin](./create-plugins.md)
- [Deephaven Express](/core/plotly/docs/)
- [deephaven.ui](/core/ui/docs/)
- [Install and use Python packages](./install-and-use-python-packages.md)
- [Install packages](./install-packages.md)
- [Launch Deephaven from pre-built images](../getting-started/docker-install.md)
- [Matplotlib and Seaborn](./plotting/matplot-seaborn.md)
- [Python client quickstart](../getting-started/pyclient-quickstart.md)
- [`PluginClient` Pydoc](/core/client-api/python/code/pydeephaven.experimental.plugin_client.html#pydeephaven.experimental.plugin_client.PluginClient)
- [`Session` Pydoc](/core/client-api/python/code/pydeephaven.session.html#pydeephaven.session.Session)
