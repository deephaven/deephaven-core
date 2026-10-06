---
title: Install and use plugins
---

This guide covers the installation and use of plugins in Deephaven. A plugin is something that extends the functionality of a software. Groovy packages are a common example of plugins - they extend Groovy's capabilities. Plugins in Deephaven can extend the functionality of a running server, UI, client API, or all of them.

Deephaven offers several pre-built plugins that can extend the platform's functionality. These are available to anyone using Deephaven Community Core. They can be installed easily and provide a range of additional capabilities. For the full list of available plugins, see [available plugins](#available-plugins).

Server-side plugins extend the functionality of the Deephaven server. For instance, plotting plugins add the ability to plot with new APIs such as Plotly Express, Matplotlib, and Seaborn. Authentication plugins add the ability to authenticate users with new authentication methods such as mTLS.

Client-side plugins extend the functionality of any of Deephaven's client APIs. For instance, the Java client API can be extended with plugins that allow the client to manage arbitrary objects in the server, or to interact with the server using a different serialization format.

Other plugins may have both a server-side plugin and a client-side plugin, allowing bidirectional communication between the client and server.

> [!NOTE]
> In some cases, you'll want to install packages rather than use plugins. Those instructions are covered in [How to install packages](./install-packages.md).
>
> To have _complete control_ of the build process, you can [Build and launch Deephaven from source code](../getting-started/launch-build.md).

This guide covers the installation and use of pre-built plugins. For information on building your own plugins, see [Create your own plugin](./create-plugins.md).

## Install a plugin

> [!NOTE]
> Authentication plugins require additional configuration, which is outside the scope of this guide. For more information, see the documentation for each authentication plugin.

### Extend Deephaven with Docker

First, follow the [Launch Deephaven from pre-built images](../getting-started/docker-install.md) steps from the Docker install guide.

The following Dockerfile provides a template for installing a plugin containing both JavaScript and server components in a Deephaven Docker image:

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

## Use plugins from the Java client

The [Java client](https://github.com/deephaven/deephaven-core/tree/main/java-client) can interact with plugin objects on the server through the `fetchable` and `bidirectional` methods of a `Session`. Each method takes a typed ticket, which pairs the plugin's object type with a reference to the object, such as a variable name in the server's scope.

The examples below assume you already have a connected `Session`. For complete, runnable programs that create a session, see the [Java client session examples](https://github.com/deephaven/deephaven-core/tree/main/java-client/session-examples/src/main/java/io/deephaven/client/examples).

### Fetch a plugin object

For plugins that send their object's contents to the client, use `fetchable`:

```java skip-test
import io.deephaven.client.impl.ObjectService.Fetchable;
import io.deephaven.client.impl.ScopeId;
import io.deephaven.client.impl.ServerData;
import io.deephaven.client.impl.TypedTicket;

// "MyPluginType" is the object type registered by the server-side plugin,
// and "plugin_object_name" is the variable name of the object on the server.
TypedTicket typedTicket = new TypedTicket("MyPluginType", new ScopeId("plugin_object_name"));

try (Fetchable fetchable = session.fetchable(typedTicket).get();
        ServerData serverData = fetchable.fetch().get()) {
    // The payload's format depends on the plugin implementation
    System.out.println("Payload size: " + serverData.data().remaining());
}
```

### Work with bidirectional plugins

For bidirectional plugins, use `bidirectional` to open a message stream to the object. You supply a `MessageStream<ServerData>` that receives messages from the server, and you send messages through the `MessageStream<ClientData>` that `connect` returns:

```java skip-test
import io.deephaven.client.impl.ClientData;
import io.deephaven.client.impl.ObjectService.Bidirectional;
import io.deephaven.client.impl.ObjectService.MessageStream;
import io.deephaven.client.impl.ServerData;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Collections;

try (Bidirectional bidirectional = session.bidirectional(typedTicket).get()) {
    MessageStream<ServerData> fromServer = new MessageStream<>() {
        @Override
        public void onData(ServerData serverData) {
            // Handle each message from the server; the format depends on the plugin
            System.out.println("Received " + serverData.data().remaining() + " bytes");
        }

        @Override
        public void onClose() {
            System.out.println("Stream closed");
        }
    };

    MessageStream<ClientData> toServer = bidirectional.connect(fromServer);
    toServer.onData(new ClientData(
            ByteBuffer.wrap("hello".getBytes(StandardCharsets.UTF_8)),
            Collections.emptyList()));
    // ... wait for the server's responses before closing ...
    toServer.onClose();
}
```

For a complete example, see [`MessageStreamSendReceive.java`](https://github.com/deephaven/deephaven-core/blob/main/java-client/session-examples/src/main/java/io/deephaven/client/examples/MessageStreamSendReceive.java).

### Gradle dependencies for plugin development

To use the Java client in your project, add the session library to your `build.gradle`, replacing `<version>` with your Deephaven version:

```gradle
dependencies {
    implementation 'io.deephaven:deephaven-java-client-session:<version>'

    // Add other plugin-specific dependencies as needed
}
```

## Related documentation

- [Access your file system with Docker data volumes](../conceptual/docker-data-volumes.md)
- [Build and launch Deephaven from source code](../getting-started/launch-build.md)
- [Create your own plugin](./create-plugins.md)
- [How to install packages](./install-packages.md)
- [Install and use Java packages](./install-and-use-java-packages.md)
- [Launch Deephaven from pre-built images](../getting-started/docker-install.md)
