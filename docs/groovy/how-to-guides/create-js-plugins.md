---
title: Create a JavaScript plugin
---

JavaScript (JS) plugins extend the Deephaven web UI with custom React components. A plugin's JavaScript code doesn't depend on the server's script language, so you write it for a Groovy server the same way you write it for a Python server.

To write that code, follow [Create a JavaScript plugin](/core/docs/how-to-guides/create-js-plugins/) in the Python documentation. That guide covers plugin types, build configuration, and registering several plugins from one package. Two parts of it don't apply on a Groovy server:

- **Element plugins and the `element` cookiecutter template.** Element plugins render [`deephaven.ui`](/core/docs/how-to-guides/deephaven-ui/) components, which only Python code can create.
- **The Python registration that both cookiecutter templates generate**, including the `widget` template's Python object type. Register the built plugin as this guide describes. If you build a widget plugin, register the object type it renders from Java, as shown in [Create your own plugin](./create-plugins.md).

Every other plugin type works on a Groovy server, including widget, dashboard, table, and theme plugins. If you build a theme plugin, see [Custom themes](./custom-themes.md).

This guide covers how to register the built plugin with a Groovy server and how to set the table attribute that a table plugin needs.

## Ways to register a JS plugin

On a Groovy server, register a JS plugin in one of the following ways:

- **Server manifest**: Package the plugin with the `web-plugin-packager` Docker image, as shown in [Configure JS plugins](./configuration/js-plugins.md), and place the generated `js-plugins` directory where the server reads it. See [Register a JS plugin with a server manifest](#register-a-js-plugin-with-a-server-manifest).
- **Java registration**: Register a [`JsPlugin`](/core/javadoc/io/deephaven/plugin/js/JsPlugin.html) from a Java [`Registration`](/core/javadoc/io/deephaven/plugin/Registration.html) class in a JAR on the server classpath. See [Register a JS plugin from Java](#register-a-js-plugin-from-java).
- **npm package directory**: During development, point a `deephaven.jsPlugins.<name>` configuration property at the plugin's npm package directory. See [Register an npm package directory during development](#register-an-npm-package-directory-during-development).

The [Python registration](/core/docs/how-to-guides/create-js-plugins/#python-registration) section of the Python documentation describes packaging a JS plugin in a Python package. The server registers plugins from Python packages only when Python is its script language, so that route doesn't work on a Groovy server.

The server registers JS plugins only at startup, so restart it after you add or change a plugin.

## Register a JS plugin with a server manifest

To register a plugin through the server manifest:

1. Package the plugin with the `pack-plugins.sh` script from the `web-plugin-packager` Docker image, as shown in [Configure JS plugins](./configuration/js-plugins.md). The script extracts each npm package and writes a `manifest.json` file that lists them. If the plugin isn't published to npm:
   1. Build the plugin.
   2. Copy its npm package directory into the packager stage, for example `COPY src/js /my-plugin`.
   3. Pass the directory's absolute path to the script, for example `RUN ./pack-plugins.sh /my-plugin`. A path relative to the working directory doesn't work, because the script runs `npm pack` from a temporary subdirectory.
2. Copy the contents of the generated `js-plugins` directory into the `js-plugins/` subdirectory of the server's [configuration directory](./configuration/configure-production-application.md#deephaven-server-bootstrap-configuration), so that `manifest.json` is at `<configDir>/js-plugins/manifest.json`. Alternatively, set the `deephaven.jsPlugins.resourceBase` configuration property to the directory that contains `manifest.json`.

The server loads only the plugins listed in `manifest.json`, so copying an npm package into the directory by hand doesn't register it. See [Configure JS plugins](./configuration/js-plugins.md).

## Register a JS plugin from Java

At startup, the server uses Java's `ServiceLoader` to find [`Registration`](/core/javadoc/io/deephaven/plugin/Registration.html) classes in the JARs on its classpath. A [`Registration`](/core/javadoc/io/deephaven/plugin/Registration.html) can register a [`JsPlugin`](/core/javadoc/io/deephaven/plugin/js/JsPlugin.html) that you build with [`JsPlugin.builder`](/core/javadoc/io/deephaven/plugin/js/JsPlugin.html#builder()):

```java
package com.example;

import io.deephaven.plugin.Registration;
import io.deephaven.plugin.js.JsPlugin;

import java.nio.file.Path;

public final class MyJsPluginRegistration implements Registration {
    @Override
    public void registerInto(Callback callback) {
        callback.register(JsPlugin.builder()
                // Contents served at js-plugins/{name}/
                .name("@my-org/my-plugin")
                .version("0.1.0")
                // Path to the main JS file, relative to path
                .main(Path.of("dist/index.js"))
                // Directory on the server that contains the built JS assets
                .path(Path.of("/opt/my-plugin"))
                .build());
    }
}
```

Copy the built plugin to `/opt/my-plugin`, so that the main file is at `/opt/my-plugin/dist/index.js`. The directory and the main file must both exist when the server starts, or building the [`JsPlugin`](/core/javadoc/io/deephaven/plugin/js/JsPlugin.html) fails.

To let `ServiceLoader` find the class, list it in `src/main/resources/META-INF/services/io.deephaven.plugin.Registration`:

```text
com.example.MyJsPluginRegistration
```

Build the JAR and add it to the server classpath. [Create your own plugin](./create-plugins.md) shows the build configuration and how to add a plugin JAR to a local or Docker server.

In Deephaven Community Core 42.5 and later, a plugin can use exports from other JS plugins by naming them in the [`loader` field](/core/docs/how-to-guides/create-js-plugins/#use-exports-from-another-js-plugin) of its `package.json`. The web UI then loads those plugins first.

The builder doesn't read `package.json`. If the plugin has a `loader` field, pass the same value to the builder's [`loader`](/core/javadoc/io/deephaven/plugin/js/JsPlugin.Builder.html#loader(java.lang.Object)) method, for example `.loader(Map.of("dependencies", List.of("@my-org/other-plugin")))`.

## Register an npm package directory during development

During development, you can skip packaging and point the server at the plugin's npm package directory. That directory contains the plugin's `package.json`.

Set a configuration property named `deephaven.jsPlugins.<name>` to that directory, in the [configuration file](./configuration/config-file.md) or as a JVM argument. For example:

```properties
deephaven.jsPlugins.my-plugin=/path/to/my-plugin/src/js
```

`<name>` can be any label that doesn't contain a `.`. Don't use `resourceBase` as the label, because `deephaven.jsPlugins.resourceBase` sets the server manifest directory.

The server reads the plugin's name, version, and main file from `package.json`. When the main file is in a subdirectory, the server serves only the main file's top-level directory, such as `dist/` for `dist/index.js`.

When you register a plugin this way:

- Remove any other registration of the same plugin, such as a server manifest entry. The server throws an error when two JS plugins have the same name.
- Build the plugin before you start the server.
- After each rebuild, restart the server and refresh the web UI. The server copies the files only when it starts.

## Set the attribute for a table plugin

A [table plugin](/core/docs/how-to-guides/create-js-plugins/#plugin-types) adds a custom component to the panel of any table whose `PluginName` attribute is set to the name the plugin is registered under. [Plugin types](/core/docs/how-to-guides/create-js-plugins/#plugin-types) in the Python documentation explains which name that is. On a Groovy server, set the attribute with [`withAttributes`](../reference/table-operations/select/withAttributes.md), for example `t.withAttributes(["PluginName": "@my-org/my-plugin"])`.

## Related documentation

- [Install and use plugins](./install-use-plugins.md)
- [Configure JS plugins](./configuration/js-plugins.md)
- [Configuration file](./configuration/config-file.md)
- [Configure the Deephaven production application](./configuration/configure-production-application.md)
- [Create your own plugin](./create-plugins.md)
- [Custom themes](./custom-themes.md)
- [Create a JavaScript plugin (Python documentation)](/core/docs/how-to-guides/create-js-plugins/)
- [`withAttributes`](../reference/table-operations/select/withAttributes.md)
- [deephaven-plugins repository](https://github.com/deephaven/deephaven-plugins)
- [`JsPlugin` Javadoc](/core/javadoc/io/deephaven/plugin/js/JsPlugin.html)
- [`Registration` Javadoc](/core/javadoc/io/deephaven/plugin/Registration.html)
