---
title: Install and use plugins
---

This guide covers how to install and use pre-built plugins in Deephaven. A plugin extends the functionality of Deephaven, much as a Java library extends what Groovy and Java code can do. A plugin's server-side components are often packaged as Java libraries, but not every Java library is a plugin. Plugins in Deephaven can extend the functionality of a running server, UI, client API, or all of them.

Deephaven offers several pre-built plugins that extend the platform's functionality, and anyone using Deephaven Community Core can install them. For the plugins you can install, see [Available plugins](#available-plugins).

> [!NOTE]
> To install Linux system packages rather than plugins, see [Install packages](./install-packages.md). To add Java libraries to the server, see [Install and use Java packages](./install-and-use-java-packages.md).
>
> To have _complete control_ of the build process, see [Build and launch Deephaven from source code](../getting-started/launch-build.md).

For information on building your own plugins, see [Create your own plugin](./create-plugins.md).

## Types of plugins

Deephaven plugins come in four types:

- **Server-side plugins** extend the Deephaven server. For example, authentication plugins add authentication methods such as [mutual TLS (mTLS)](./authentication/auth-mtls.md). Some server-side plugins, such as the plotting plugins for [Deephaven Express](/core/plotly/docs/) and [Matplotlib](/core/docs/how-to-guides/plotting/matplot-seaborn/), are Python packages that run only on a server with Python as its scripting language.
- **UI plugins**, also called JavaScript (JS) plugins, extend the Deephaven web IDE. They are JavaScript packages that the server loads and serves to the browser. For example, the [theme pack](https://github.com/deephaven/deephaven-plugins/tree/main/plugins/theme-pack) adds IDE color themes.
- **Client-side plugins** extend Deephaven's client APIs. For example, a plugin can extend the [Python client](/core/docs/getting-started/pyclient-quickstart/) so it can work with a plugin's objects on the server. The [Java client API](./java-client.md) doesn't load plugins, but it can fetch plugin objects from the server and exchange messages with them in a format the plugin defines. See [Use plugins from the Java client](#use-plugins-from-the-java-client).
- **Bidirectional plugins** keep a message stream open between a server-side object and a client so that the two can exchange messages. Some, like the Python examples in [Bidirectional plugin examples](#bidirectional-plugin-examples), also ship a client-side plugin. The Java client can talk to a bidirectional plugin directly.

## Install a plugin

> [!NOTE]
> Authentication plugins need additional configuration. See [Authentication](#authentication) for the guide to each one.

On a Groovy server, how you install a plugin depends on how it is packaged:

- **Server-side components** are Java libraries on the server's classpath. To add them, see [Install and use Java packages](./install-and-use-java-packages.md).
- **JS components published to npm** need packaging. Package them with `pack-plugins.sh` and copy the result to the directory where the server looks for JS plugins. In Docker, build a custom image, as described in the next section. For other installations, see [Configure JS plugins](./configuration/js-plugins.md).

The server loads plugins only when it starts, so restart the server after you install a plugin.

### Extend Docker images for JS plugins

First, follow the steps in [Launch Deephaven from pre-built images](../getting-started/docker-install.md).

The `server-slim` image runs a Groovy console. To add JS plugins to it, build a custom image in two stages:

1. The first stage uses the `web-plugin-packager` image to download plugins from npm.
2. The second stage copies them into `server-slim`.

Each plugin's npm package name is the `name` field of `src/js/package.json` in its folder of the [plugins repository](#the-plugins-repository). You can also [search npm for the `deephaven-js-plugin` keyword](https://www.npmjs.com/search?q=keywords%3Adeephaven-js-plugin).

The following Dockerfile is the general template:

```docker title="Dockerfile"
FROM ghcr.io/deephaven/web-plugin-packager:latest AS js-plugins
# 1. Package the npm deephaven-js-plugin(s)
RUN ./pack-plugins.sh <plugins>

FROM ghcr.io/deephaven/server-slim:latest
# 2. Copy the js-plugins/ directory
COPY --from=js-plugins js-plugins/ /opt/deephaven/config/js-plugins/
```

Replace `<plugins>` with one or more npm package names, separated by spaces. To pin a version, append `@<version>` to a name.

For more about JS plugin packaging and configuration, see [Configure JS plugins](./configuration/js-plugins.md).

#### Example: install the theme pack

The [theme pack](https://github.com/deephaven/deephaven-plugins/tree/main/plugins/theme-pack) plugin adds UI themes such as Dracula, Night Owl, and Solarized Dark to the Deephaven IDE. The theme pack has no server-side logic. Its npm package, `@deephaven/js-plugin-theme-pack`, needs no Python package, so a Groovy server can use it.

1. Create a directory for your deployment and save the following as `Dockerfile` in it:

   ```docker title="Dockerfile"
   FROM ghcr.io/deephaven/web-plugin-packager:latest AS js-plugins
   RUN ./pack-plugins.sh @deephaven/js-plugin-theme-pack

   FROM ghcr.io/deephaven/server-slim:latest
   COPY --from=js-plugins js-plugins/ /opt/deephaven/config/js-plugins/
   ```

2. Build the image and run it:

   ```bash
   docker build -t deephaven-groovy-themes .
   docker run --rm -p 10000:10000 deephaven-groovy-themes
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

   Then run `docker compose up --build` instead of the two `docker` commands.

3. Confirm that the server loaded the plugin. The server lists its JS plugins at `http://localhost:10000/js-plugins/manifest.json`:

   ```bash
   curl http://localhost:10000/js-plugins/manifest.json
   ```

   The output includes `@deephaven/js-plugin-theme-pack`.

4. Open the IDE at `http://localhost:10000/ide` and log in with the [pre-shared key](./authentication/auth-psk.md) printed in the server log. Then click the gear icon at the top right of the IDE to open the **Settings** menu. Expand the **Theme** section and pick one of the added themes from the **Pick a color scheme** list.

## Use plugins from the Java client

The [Java client](./java-client.md) can interact with plugin objects on the server through the [`fetchable`](/core/javadoc/io/deephaven/client/impl/ObjectService.html#fetchable(io.deephaven.client.impl.HasTypedTicket)) and [`bidirectional`](/core/javadoc/io/deephaven/client/impl/ObjectService.html#bidirectional(io.deephaven.client.impl.HasTypedTicket)) methods of a [`Session`](/core/javadoc/io/deephaven/client/impl/Session.html). Each method takes a [typed ticket](/core/javadoc/io/deephaven/client/impl/TypedTicket.html), which pairs the plugin's object type with a reference to the object, such as a variable name in the server's scope.

### Set up the Java client

To use the [Java client](./java-client.md) in your project, add one client library to your `build.gradle`, replacing `<version>` with your Deephaven version:

- `deephaven-java-client-session` if you create a [`Session`](/core/javadoc/io/deephaven/client/impl/Session.html) directly.
- `deephaven-java-client-barrage-dagger` if you already use a [`BarrageSession`](/core/javadoc/io/deephaven/client/impl/BarrageSession.html) for table data, as in [Connect to the server](./java-client.md#connect-to-the-server).

The following example adds the session library:

```gradle
dependencies {
    implementation 'io.deephaven:deephaven-java-client-session:<version>'

    // Add other plugin-specific dependencies as needed
}
```

The examples below assume a connected [`Session`](/core/javadoc/io/deephaven/client/impl/Session.html) in a variable named `session`. To get one:

- With only the session library, create a [`Session`](/core/javadoc/io/deephaven/client/impl/Session.html) from a [`SessionFactoryConfig`](/core/javadoc/io/deephaven/client/impl/SessionFactoryConfig.html), as the [Java client session examples](https://github.com/deephaven/deephaven-core/tree/main/java-client/session-examples/src/main/java/io/deephaven/client/examples) do.
- With a [`BarrageSession`](/core/javadoc/io/deephaven/client/impl/BarrageSession.html), as in [Connect to the server](./java-client.md#connect-to-the-server), call its [`session`](/core/javadoc/io/deephaven/client/impl/FlightSession.html#session()) method, which it inherits from [`FlightSession`](/core/javadoc/io/deephaven/client/impl/FlightSession.html).

### Fetch a plugin object

For plugins that send their object's contents to the client, use [`fetchable`](/core/javadoc/io/deephaven/client/impl/ObjectService.html#fetchable(io.deephaven.client.impl.HasTypedTicket)):

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

For bidirectional plugins, use [`bidirectional`](/core/javadoc/io/deephaven/client/impl/ObjectService.html#bidirectional(io.deephaven.client.impl.HasTypedTicket)) to open a message stream to the object. This example reuses the `typedTicket` created in [Fetch a plugin object](#fetch-a-plugin-object). The stream has two directions:

- You supply a [`MessageStream<ServerData>`](/core/javadoc/io/deephaven/client/impl/ObjectService.MessageStream.html) that receives messages from the server.
- You send messages through the [`MessageStream<ClientData>`](/core/javadoc/io/deephaven/client/impl/ObjectService.MessageStream.html) that [`connect`](/core/javadoc/io/deephaven/client/impl/ObjectService.Bidirectional.html#connect(io.deephaven.client.impl.ObjectService.MessageStream)) returns.

The following example opens a stream, sends one message, and prints the size of each message the server sends back:

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
            // Handle each message from the server; the format depends on the plugin.
            // Closing the message releases any server objects it exports.
            try (serverData) {
                System.out.println("Received " + serverData.data().remaining() + " bytes");
            }
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

For a complete example, see [`MessageStreamSendReceive.java`](https://github.com/deephaven/deephaven-core/blob/main/java-client/session-examples/src/main/java/io/deephaven/client/examples/tools/MessageStreamSendReceive.java).

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

The [theme pack](https://github.com/deephaven/deephaven-plugins/tree/main/plugins/theme-pack) has no server-side logic, so a Groovy server can use its npm package. See [Example: install the theme pack](#example-install-the-theme-pack).

The following user interface plugins are Python packages for servers with Python as their scripting language. The `ghcr.io/deephaven/server` image already includes them, but the Groovy `server-slim` image can't use them:

- [`deephaven-plugin-ui`](https://pypi.org/project/deephaven-plugin-ui/): A plugin for building real-time dashboards with [deephaven.ui](/core/ui/docs/).
- [`deephaven-plugin-plotly-express`](https://pypi.org/project/deephaven-plugin-plotly-express/): A plugin, [Deephaven Express](/core/plotly/docs/), that makes [Plotly Express](https://plotly.com/python/plotly-express/) compatible with Deephaven tables.
- [`deephaven-plugin-matplotlib`](https://pypi.org/project/deephaven-plugin-matplotlib/): A plugin that makes [Matplotlib](https://matplotlib.org/) and [Seaborn](https://seaborn.pydata.org/) compatible with Deephaven tables.

### Authentication

Authentication plugins have a more complex installation process than other plugins. See the following guides for details:

- [Keycloak](./authentication/auth-keycloak.md): A plugin that enables the use of [Keycloak](https://www.keycloak.org/) and [OpenID Connect (OIDC)](https://openid.net/developers/how-connect-works/) for authentication.
- [mTLS](./authentication/auth-mtls.md): A plugin that enables [mutual TLS (mTLS)](https://www.cloudflare.com/learning/access-management/what-is-mutual-tls/) authentication.
- [Username/password](./authentication/auth-uname-pw.md): A plugin that enables username/password authentication.

### Bidirectional plugin examples

Bidirectional plugins let you define custom remote procedure call (RPC) methods that clients call to interact with objects on a running server and get objects back. The following examples are Python plugins, with a Python server plugin and a Python client plugin:

- [Pickle RPC plugin](https://github.com/deephaven-examples/plugin-python-rpc-pickle): A plugin to remotely execute methods on a Deephaven server. Its client code imports `pydeephaven.experimental.server_object`, which `pydeephaven` removed in version 0.37.0, so it needs updating before it runs with current clients. It uses Python's `pickle` module, which is not secure. Read its README before you adapt it.
- [Example bidirectional plugin](https://github.com/deephaven-examples/plugin-bidirectional-example): A minimal bidirectional plugin and a client that uses it. Its client code imports `pydeephaven.experimental.server_object`, which `pydeephaven` removed in version 0.37.0. For client code that works with current versions, see the [Python version of Create your own plugin](/core/docs/how-to-guides/create-plugins/).

A Groovy server can also host bidirectional plugins written in Java or Groovy. Such a plugin extends [`ObjectTypeBase`](/core/javadoc/io/deephaven/plugin/type/ObjectTypeBase.html) and implements its [`compatibleClientConnection`](/core/javadoc/io/deephaven/plugin/type/ObjectTypeBase.html#compatibleClientConnection(java.lang.Object,io.deephaven.plugin.type.ObjectType.MessageStream)) method to return a [`MessageStream`](/core/javadoc/io/deephaven/plugin/type/ObjectType.MessageStream.html). To learn more:

- For a fetch-only plugin, and how a bidirectional plugin differs from one, see [Create your own plugin](./create-plugins.md).
- For a complete bidirectional plugin, see the [Python version of Create your own plugin](/core/docs/how-to-guides/create-plugins/).
- To use a plugin object from Java, see [Use plugins from the Java client](#use-plugins-from-the-java-client).

## Related documentation

- [Build and launch Deephaven from source code](../getting-started/launch-build.md)
- [Configure and use pre-shared key authentication](./authentication/auth-psk.md)
- [Configure JS plugins](./configuration/js-plugins.md)
- [Configure Keycloak for authentication](./authentication/auth-keycloak.md)
- [Configure mTLS for Deephaven](./authentication/auth-mtls.md)
- [Configure the Deephaven Docker application](./configuration/docker-application.md)
- [Configure username/password authentication](./authentication/auth-uname-pw.md)
- [Create your own plugin](./create-plugins.md)
- [Extract data with a Java client](./java-client.md)
- [Install and use Java packages](./install-and-use-java-packages.md)
- [Install packages](./install-packages.md)
- [Launch Deephaven from pre-built images](../getting-started/docker-install.md)
- [Python client quickstart](/core/docs/getting-started/pyclient-quickstart/)
- [`BarrageSession` Javadoc](/core/javadoc/io/deephaven/client/impl/BarrageSession.html)
- [`ClientData` Javadoc](/core/javadoc/io/deephaven/client/impl/ClientData.html)
- [`FlightSession` Javadoc](/core/javadoc/io/deephaven/client/impl/FlightSession.html)
- [`ObjectService` Javadoc](/core/javadoc/io/deephaven/client/impl/ObjectService.html)
- [`ObjectType.MessageStream` Javadoc](/core/javadoc/io/deephaven/plugin/type/ObjectType.MessageStream.html)
- [`ObjectTypeBase` Javadoc](/core/javadoc/io/deephaven/plugin/type/ObjectTypeBase.html)
- [`ScopeId` Javadoc](/core/javadoc/io/deephaven/client/impl/ScopeId.html)
- [`ServerData` Javadoc](/core/javadoc/io/deephaven/client/impl/ServerData.html)
- [`Session` Javadoc](/core/javadoc/io/deephaven/client/impl/Session.html)
- [`SessionFactoryConfig` Javadoc](/core/javadoc/io/deephaven/client/impl/SessionFactoryConfig.html)
- [`TypedTicket` Javadoc](/core/javadoc/io/deephaven/client/impl/TypedTicket.html)
