---
title: Install and use plugins
---

There are many ways to customize either the Deephaven build or packages to fit your use-cases. In this guide, we extend Deephaven to build a custom Docker image with JS plugins installed. For this guide, we will add the `js-plugin-matplotlib`. This makes the popular [Matplotlib](https://matplotlib.org/) library available for use, providing more data visualization options within the Deephaven IDE, such as 3D plots and sophisticated scatter plots. As always, you'll be able to plot both your static and real-time data.

Plugins in Deephaven can extend the functionality of a running server, UI, client API, or all of them. Deephaven offers several pre-built plugins that can extend the platform's functionality. These are available to anyone using Deephaven Community Core.

Server-side plugins extend the functionality of the Deephaven server. For instance, plotting plugins add the ability to plot with new APIs such as Plotly Express, Matplotlib, and Seaborn. Authentication plugins add the ability to authenticate users with new authentication methods such as mTLS.

Client-side plugins extend the functionality of any of Deephaven's client APIs. For instance, the Groovy client API can be extended with plugins that allow the client to manage arbitrary objects in the server, or to interact with the server using a different serialization format.

> [!NOTE]
> In some cases, you'll want to install packages rather than use plugins. Those instructions are covered in [How to install packages](./install-packages.md).
>
> To have _complete control_ of the build process, you can [Build and launch Deephaven from source code](./launch-build.md).

This guide covers the installation and use of pre-built plugins. For information on building your own plugins, see [Create your own plugin](./create-plugins.md).

## Install a plugin

> [!NOTE]
> Authentication plugins require additional configuration, which is outside the scope of this guide. For more information, see the documentation for each authentication plugin.

### Extend Deephaven with Docker

First, follow the [Launch Deephaven from pre-built images](../tutorials/docker-install.md) steps from the Docker install guide.

To open a Groovy session, run:

```bash
compose_file=https://github.com/deephaven/deephaven-core/blob/main/containers/groovy-examples/docker-compose.yml
curl  -O "${compose_file}"
```

Once you have the `docker-compose.yml` file pulled down, define your own web Docker image in `web/Dockerfile` that includes the plugins you would like to use.

1. Create the subdirectory `web` in the same folder as your `docker-compose.yml`:
   `mkdir web`
2. Create the `Dockerfile` for web and open for editing:
   `vi web/Dockerfile`
3. Paste the following into the `web/Dockerfile` and save:

   ```bash
   # Pull the web-plugin-packager image
   FROM ghcr.io/deephaven/web-plugin-packager:main as build

   # Specify the plugins you wish to use. You can specify multiple plugins separated by a space, and optionally include the version number, e.g.
   # RUN ./pack-plugins.sh <js-plugin-name>[@version] ...
   # For a list of published plugins, see https://www.npmjs.com/search?q=keywords%3Adeephaven-js-plugin

   # Here is how you would install the matplotlib and table-example plugins
   RUN ./pack-plugins.sh @deephaven/js-plugin-matplotlib @deephaven/js-plugin-table-example

   # Copy the packaged plugins over
   FROM ghcr.io/deephaven/web:${VERSION:-latest}
   COPY --from=build js-plugins/ /usr/share/nginx/html/js-plugins/
   ```

Many plugins will also require a server side component. To define the plugins used on the server, create a `server/Dockerfile` similar to above:

1. Create subdirectory `server` in the same folder as your `docker-compose.yml`:
   `mkdir server`
2. Create the `Dockerfile` for server and open for editing:
   `vi server/Dockerfile`
3. Paste the following into the `server/Dockerfile` and save:

   ```bash
   FROM ghcr.io/deephaven/server:${VERSION:-latest}
   # pip install any of the plugins required on the server
   RUN pip install deephaven-plugin-matplotlib
   ```

After building, you need to update your `docker-compose` file to specify using that build. Modify the existing `docker-compose.yml` file and replace the web and server definitions with the following:

```yaml
services:
  web:
    build:
      context: ./web
    ports:
      - "${WEB_PORT:-8080}:80"
  grpc-proxy:
    image: ghcr.io/deephaven/grpc-proxy:${VERSION:-latest}
    environment:
      - BACKEND_ADDR=server:8080
    depends_on:
      - server
    ports:
      - "${DEEPHAVEN_PORT:-10000}:8080"
  server:
    build:
      context: ./server
    expose:
      - "8080"
    volumes:
      - ./data:/data
    environment:
      - JAVA_TOOL_OPTIONS=-Xmx4g -Ddeephaven.console.type=groovy -Ddeephaven.application.dir=/opt/deephaven/config
```

When you're done, your directory structure should look like:

```
.
├── docker-compose.yml
├── server
│   └── Dockerfile
└── web
    └── Dockerfile
```

Everything's ready to go! Now you just need to run `docker compose up` as normal, and you will be using your custom image with your JS plugins installed.

### Alternative Docker template

The following Dockerfile provides an alternative template for installing a plugin containing both Javascript and server components in a Deephaven Docker image:

```docker title="Dockerfile"
FROM ghcr.io/deephaven/web-plugin-packager:main as js-plugins
# 1. Package the NPM deephaven-js-plugin(s)
RUN ./pack-plugins.sh <plugins>

FROM ghcr.io/deephaven/server:main
# 2. Install the server-side plugin components if necessary (some plugins may be JS only)
RUN pip install --no-cache-dir <packages>
# 3. Copy the js-plugins/ directory
COPY --from=js-plugins js-plugins/ /opt/deephaven/config/js-plugins/
```

You can use Docker to build and run the image:

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

### User interface

All of the following user interface plugins can be installed with `pip` with no extra work.

- [`deephaven-plugin-ui`](https://pypi.org/project/deephaven-plugin-ui/): A plugin for real-time dashboards.
- [`deephaven-plugin-plotly-express`](https://pypi.org/project/deephaven-plugin-plotly-express/): A plugin that makes [Plotly Express](https://plotly.com/python/plotly-express/) compatible with Deephaven tables.
- [`deephaven-plugin-matplotlib`](https://pypi.org/project/deephaven-plugin-matplotlib/): A plugin that makes [Matplotlib](https://matplotlib.org/) and [Seaborn](https://seaborn.pydata.org/) compatible with Deephaven tables.

### Authentication

Authentication plugins have a more complex installation process than other plugins. Please refer to the documentation links below for more information.

- [Keycloak](./authentication/auth-keycloak.md): A plugin that enables the use of [Keycloak](https://www.keycloak.org/) and [OpenID Connect (OIDC)](https://openid.net/developers/how-connect-works/) for authentication.
- [mTLS](./authentication/auth-mtls.md): A plugin that enables [mutual TLS (mTLS)](https://www.cloudflare.com/learning/access-management/what-is-mutual-tls/) authentication.
- [Username/password](./authentication/auth-uname-pw.md): A plugin that enables username/password authentication.

### Bidirectional plugin examples

[Bidirectional plugins](./create-plugins.md) allow users to create custom RPC methods that enable clients to interact with and return objects on a running server. See the following for an example:

- [Pickle RPC plugin](https://github.com/deephaven-examples/plugin-python-rpc-pickle): A plugin to remotely execute methods on a Deephaven server.
- [Example bidirectional plugin](https://github.com/deephaven-examples/plugin-bidirectional-example): The same plugin presented in the [Bidirectional plugins guide](./create-plugins.md).

## Using plugins with the Groovy client

When working with plugins from the Groovy client API, you'll typically interact with them through the session's plugin client functionality. Here's a general pattern for using plugins:

### Basic plugin usage

```groovy skip-test
import io.deephaven.client.impl.Session
import io.deephaven.client.impl.SessionConfig
import io.deephaven.client.impl.authentication.ConfigAuthenticationHandler

// Create a session configuration
def config = SessionConfig.builder()
    .target("localhost:10000")
    .authenticationHandler(ConfigAuthenticationHandler.anonymous())
    .build()

// Create and connect the session
def session = Session.connect(config).get()

try {
    // List available exportable objects (which may include plugin objects)
    println("Available objects: ${session.exportableObjects().keySet()}")

    // Get a specific plugin object
    def pluginObjectTicket = session.exportableObjects().get("plugin_object_name")

    if (pluginObjectTicket != null) {
        // Create a plugin client for the object
        def pluginClient = session.pluginClient(pluginObjectTicket)

        // Use the plugin client as needed
        // (specific usage depends on the plugin implementation)

        pluginClient.close()
    }

} finally {
    session.close()
}
```

### Working with bidirectional plugins

For bidirectional plugins that support custom RPC methods, you'll typically create a proxy class that wraps the plugin client:

```groovy skip-test
// Assuming you have a plugin proxy class like ExampleServiceProxy
def pluginClient = session.pluginClient(pluginObjectTicket)
def pluginProxy = new ExampleServiceProxy(pluginClient)

try {
    // Call methods on the plugin
    def result = pluginProxy.someMethod("parameter")
    println("Plugin result: ${result}")

} finally {
    pluginProxy.close()
}
```

### Gradle dependencies for plugin development

When developing Groovy clients that use plugins, you'll typically need these dependencies in your `build.gradle`:

```gradle
dependencies {
    implementation 'io.deephaven:deephaven-client-api:0.36.1'
    implementation 'org.apache.groovy:groovy-all:4.0.15'
    implementation 'org.apache.groovy:groovy-json:4.0.15'

    // Add other plugin-specific dependencies as needed
}
```

## Related documentation

- [Access your file system with Docker data volumes](../conceptual/docker-data-volumes.md)
- [Build and launch Deephaven from source code](./launch-build.md)
- [Create your own plugin](./create-plugins.md)
- [How to install packages](./install-packages.md)
- [Install and use Java packages](./install-and-use-java-packages.md)
- [Launch Deephaven from pre-built images](../tutorials/docker-install.md)
