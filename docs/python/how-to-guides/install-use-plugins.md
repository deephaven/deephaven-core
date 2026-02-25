---
title: Install and use plugins
---

This guide covers the installation and use of plugins in Deephaven. A plugin is something that extends the functionality of a software. Python packages are a common example of plugins - they extend Python's capabilities. Plugins in Deephaven can extend the functionality of a running server, UI, client API, or all of them.

Deephaven offers several pre-built plugins that can extend the platform's functionality. These are available to anyone using Deephaven Community Core. They can be installed easily and provide a range of additional capabilities. For the full list of available plugins, see [available plugins](#available-plugins).

Server-side plugins extend the functionality of the Deephaven server. For instance, plotting plugins add the ability to plot with new APIs such as Plotly Express, Matplotlib, and Seaborn. Authentication plugins add the ability to authenticate users with new authentication methods such as mTLS.

Client-side plugins extend the functionality of any of Deephaven's client APIs. For instance, the Python client API can be extended with plugins that allow the client to manage arbitrary Python objects in the server, or to interact with the server using a different serialization format.

Other plugins may have both a server-side plugin and a client-side plugin. The [Pickle RPC plugin](https://github.com/deephaven-examples/plugin-python-rpc-pickle) is one such example. It allows a client to make RPC calls on a server.

This guide covers the installation and use of pre-built plugins. For information on building your own plugins, see [Create your own plugin](./create-plugins.md).

## Install a plugin

> [!NOTE]
> Authentication plugins require additional configuration, which is outside the scope of this guide. For more information, see the documentation for each authentication plugin.

### pip

Some plugins can simply be installed with `pip` like any other Python package. For more information on installing Python packages in Deephaven, see [Install and use Python packages](./install-and-use-python-packages.md). For a full list of available plugins, see [available plugins](#available-plugins) below.

### Extend Docker images for JS plugins

Plugins may need more setup than just using `pip install`. This is especially true for plugins with JavaScript (JS) or other components that are not in the Python `.whl` file. The easiest way to install these plugins is to create a new Docker image based on Deephaven and add the plugin.

The following Dockerfile provides a template for installing a plugin containing both Javascript and Python components in a Deephaven Docker image:

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

You can use [Docker](./configuration/docker-application.md) to build and run the image:

```bash
docker build -t my-deephaven-image .
docker run --rm -p 10000:10000 my-deephaven-image
```

If you are using [Docker Compose](https://docs.docker.com/compose/), modify the `docker-compose.yml` file to build from a Dockerfile rather than pull the image from the registry:

```yaml title="docker-compose.yml"
services:
  deephaven:
    build:
      context: .
```

From there, you can build and run with a single command:

```bash
docker compose up
```

## Available plugins

### The plugins repository

Deephaven hosts a [plugins repository](https://github.com/deephaven/deephaven-plugins) that contains many of the official plugins offered. It's a good place to find more information about the plugins and view their source code.

> [!NOTE]
> Some plugins are not in the plugins repository but are still available for use. For instance, some authentication plugins are JAR files only available on Maven Central.

Two folders in the plugins directory are of particular interest:

- [plugins](https://github.com/deephaven/deephaven-plugins/tree/main/plugins): Contains the implementation of the plugins. This is where you can find each plugin's source code and additional documentation.
- [templates](https://github.com/deephaven/deephaven-plugins/tree/main/templates): Contains templates for creating new plugins. This is a great starting point if you want to [create your own plugin](./create-plugins.md).

The available plugins are divided into sections below based on their functionality.

### User Interface

All of the following user interface plugins can be installed with `pip` with no extra work.

- [`deephaven-plugin-ui`](https://pypi.org/project/deephaven-plugin-ui/): A plugin for real-time dashboards.
- [`deephaven-plugin-plotly-express`](https://pypi.org/project/deephaven-plugin-plotly-express/): A plugin that makes [Plotly Express](https://plotly.com/python/plotly-express/) compatible with Deephaven tables.
- [`deephaven-plugin-matplotlib`](./plotting/matplot-seaborn.md): A plugin that makes [Matplotlib](https://matplotlib.org/) and [Seaborn](https://seaborn.pydata.org/) compatible with Deephaven tables.

### Remote File Sourcing

This plugin can be installed with `pip`:

- [deephaven-plugin-python-remote-file-source](https://pypi.org/project/deephaven-plugin-python-remote-file-source/): A Deephaven bi-directional plugin to allow sourcing Python imports from a remote file source. Use with the [Deephaven VS Code extension](https://deephaven.io/vscode/docs/) to enable local Python workspaces.

### Authentication

Authentication plugins have a more complex installation process than other plugins. Please refer to the documentation links below for more information.

- [Keycloak](./authentication/auth-keycloak.md): A plugin that enables the use of [Keycloak](https://www.keycloak.org/) and [OpenID Connect (OIDC)](https://openid.net/developers/how-connect-works/) for authentication.
- [mTLS](./authentication/auth-mtls.md): A plugin that enables [mutual TLS (mTLS)](https://www.cloudflare.com/learning/access-management/what-is-mutual-tls/) authentication.
- [Username/password](./authentication/auth-uname-pw.md): A plugin that enables username/password authentication.

### Bidirectional plugin examples

[Bidirectional plugins](./create-plugins.md) allow users to create custom RPC methods that enable clients to interact with and return objects on a running server. See the following for an example:

- [Pickle RPC plugin](https://github.com/deephaven-examples/plugin-python-rpc-pickle): A plugin to remotely execute methods on a Deephaven server.
- [Example bidirectional plugin](https://github.com/deephaven-examples/plugin-bidirectional-example): The same plugin presented in the [Bidirectional plugins guide](./create-plugins.md).

## Related documentation

- [Access your file system with Docker data volumes](../conceptual/docker-data-volumes.md)
- [Build and launch Deephaven from source code](../getting-started/launch-build.md)
- [Create your own plugin](./create-plugins.md)
- [How to install packages](./install-packages.md)
- [Install and use Java packages](./install-and-use-java-packages.md)
- [Launch Deephaven from pre-built images](../getting-started/docker-install.md)
