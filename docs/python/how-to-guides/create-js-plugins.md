---
title: Create a JavaScript plugin
---

This guide covers creating JavaScript (JS) plugins that extend the Deephaven web UI with custom React components. For server-side plugins, such as plugins that register object types or add custom RPC methods to the Python client API, see [Create your own plugin](./create-plugins.md).

The Deephaven server serves each registered JS plugin's JavaScript, CSS, and other static assets, and the web UI loads those plugins when it starts.

## Prerequisites

Before creating a JS plugin, you should be familiar with:

- [React](https://react.dev/) and TypeScript/JavaScript
- [npm](https://www.npmjs.com/) package management
- Python packaging basics

## When to create a JS plugin

Create a JS plugin when you need to:

- Add custom visualization components to the Deephaven web UI.
- Integrate third-party charting or UI libraries, such as D3 or Chart.js.
- Create reusable UI components that you can share across projects.
- Build components that require complex client-side interactivity.

If you only need custom UI for a single project without sharing, consider using [`deephaven.ui`](./deephaven-ui.md) components directly.

## Quick start with cookiecutter

The easiest way to create a JS plugin is with the [cookiecutter](https://cookiecutter.readthedocs.io/) templates from the [deephaven-plugins](https://github.com/deephaven/deephaven-plugins) repository:

```bash
pip install cookiecutter
cookiecutter gh:deephaven/deephaven-plugins --directory="templates/element"
```

This creates a complete project with Python registration, React scaffolding, and build configuration.

The `element` template creates an [element plugin](#plugin-types) that extends [`deephaven.ui`](./deephaven-ui.md). An element plugin is the right choice for most plugins. If your plugin renders a server-side object that a server plugin registers, use `--directory="templates/widget"` instead to create a [widget plugin](#plugin-types).

The examples on this page use the layout that the cookiecutter templates generate:

- `src/js/` is the JavaScript project. `package.json` and the Vite configuration file are at its top, and the source is in `src/js/src/`.
- `src/my_plugin/` is the Python package.

The examples differ from the templates in these ways:

- The templates split the entry point into `src/js/src/index.ts` and a separate plugin file. The element example on this page defines the plugin directly in `src/js/src/index.tsx`. The widget and [`MultiPlugin`](#register-multiple-plugins-from-one-package) examples use separate plugin files, as the templates do.
- The templates name the Vite configuration file `vite.config.js`. This page uses `vite.config.ts`.
- The templates put the registration class in `register.py`, declare the entry point in `setup.cfg`, and copy the built JS into the package's `_js` directory. This page puts the registration class in `src/my_plugin/__init__.py`, declares the entry point in `pyproject.toml`, and copies the built JS into `src/my_plugin/js`.
- The templates create the plugin with `create_js_plugin` from [`deephaven-plugin-utilities`](https://pypi.org/project/deephaven-plugin-utilities/). This page subclasses [`JsPlugin`](/core/pydoc/code/deephaven.plugin.js.html#deephaven.plugin.js.JsPlugin) so that each property is visible. See [Python registration](#python-registration).

## Write the plugin

The JavaScript side of a plugin is a React project whose main file exports a plugin object. This section covers the plugin types and how to export several plugins from one package.

### Plugin types

The main file of a JS plugin must have a default export that the web UI can load. The source of that file is `src/js/src/index.tsx`, and the build writes it to `src/js/dist/index.js`. The [Python registration](#python-registration) names `dist/index.js` as the plugin's main file.

The default export is a plugin object that tells the web UI what kind of plugin it is and which React components to use. Every plugin object has these properties:

- **`name`**: Identifies the plugin and must be unique.
- **`type`**: One of the `PluginType` values from the `@deephaven/plugin` package. It determines which other properties the web UI expects.

The name that the web UI registers a plugin under depends on where the plugin object is:

- **The package's default export** is registered under the name from the server registration, not its own `name`. The registration uses the `package.json` `name`, unless it sets a different one, as a [`JsPlugin`](/core/pydoc/code/deephaven.plugin.js.html#deephaven.plugin.js.JsPlugin) subclass can. Set both to the package name.
- **A plugin inside a [`MultiPlugin`](#register-multiple-plugins-from-one-package)** is registered under its own `name`.

For the full set of properties each type accepts, see [`PluginTypes.ts`](https://github.com/deephaven/web-client-ui/blob/main/packages/plugin/src/PluginTypes.ts) in the web-client-ui repository.

| Type                           | Purpose                                                                                                                                                                                                                                                                                                                     |
| ------------------------------ | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `PluginType.ELEMENT_PLUGIN`    | Maps custom element names to React components for [`deephaven.ui`](./deephaven-ui.md).                                                                                                                                                                                                                                      |
| `PluginType.WIDGET_PLUGIN`     | Renders a server-side object in a panel. The `supportedTypes` property lists the server object types the plugin handles, and `component` renders them.                                                                                                                                                                      |
| `PluginType.DASHBOARD_PLUGIN`  | Mounts a `component` once per dashboard. Use it to register custom panel types or respond to dashboard events.                                                                                                                                                                                                              |
| `PluginType.TABLE_PLUGIN`      | Adds a custom `component` to the panel of any table whose `PluginName` attribute is set to the name the plugin is registered under. Set the attribute on the server with [`with_attributes`](../reference/table-operations/create/withAttributes.md), for example `t.with_attributes({"PluginName": "@my-org/my-plugin"})`. |
| `PluginType.THEME_PLUGIN`      | Provides one or more custom `themes`. See [Custom themes](./custom-themes.md).                                                                                                                                                                                                                                              |
| `PluginType.AUTH_PLUGIN`       | Adds a login method to the web UI.                                                                                                                                                                                                                                                                                          |
| `PluginType.MIDDLEWARE_PLUGIN` | Wraps the component of a widget plugin to add behavior without replacing it. Requires Deephaven Community Core 41.7 or later (web UI 1.19.0 or later).                                                                                                                                                                      |
| `PluginType.MULTI_PLUGIN`      | Bundles several of the plugins above into one package. See [Register multiple plugins from one package](#register-multiple-plugins-from-one-package).                                                                                                                                                                       |

The following `src/js/src/index.tsx` exports an element plugin, the kind of plugin the `element` cookiecutter template generates. Each key in `mapping` is an element name that a [`deephaven.ui`](./deephaven-ui.md) component on the server refers to, and each value is the React component that renders it:

```typescript
import { type ElementPlugin, PluginType } from "@deephaven/plugin";
import { MyElement } from "./MyElement";

const MyElementPlugin: ElementPlugin = {
  name: "@my-org/my-plugin",
  type: PluginType.ELEMENT_PLUGIN,
  mapping: {
    "my_plugin.MyElement": MyElement,
  },
};

export default MyElementPlugin;
```

A widget plugin renders a server-side object directly. Its `supportedTypes` value must match the name of an object type registered on the server. If no plugin registers a matching object type, the widget plugin never renders anything.

Any server plugin can register that object type. It can be written in Python or Java, and it doesn't have to ship in the same package as the JS plugin. To register an object type, see [Create your own plugin](./create-plugins.md).

The following `src/js/src/MyWidgetPlugin.tsx` defines a widget plugin:

```typescript
import { vsGraph } from "@deephaven/icons";
import { PluginType, type WidgetPlugin } from "@deephaven/plugin";
import { MyWidget } from "./MyWidget";

export const MyWidgetPlugin: WidgetPlugin = {
  name: "@my-org/my-plugin",
  type: PluginType.WIDGET_PLUGIN,
  supportedTypes: "my_plugin.MyObject",
  component: MyWidget,
  icon: vsGraph,
};

export default MyWidgetPlugin;
```

To use it as the package's only plugin, make it the default export of `src/js/src/index.tsx`:

```typescript
export { default } from "./MyWidgetPlugin";
```

### Register multiple plugins from one package

A package's default export normally registers a single plugin. To register several plugins from one package, export a [`MultiPlugin`](https://github.com/deephaven/web-client-ui/blob/main/packages/plugin/src/PluginTypes.ts) instead. A `MultiPlugin` is a plugin object whose `type` is [`PluginType.MULTI_PLUGIN`](https://github.com/deephaven/web-client-ui/blob/main/packages/plugin/src/PluginTypes.ts) and that has a `plugins` array.

When the web UI loads the package, it registers each plugin in the array individually, under that plugin's own `name`. It doesn't register the `MultiPlugin` itself, so an inner plugin can reuse the `MultiPlugin`'s `name`.

A `MultiPlugin` is useful when a package needs to:

- Provide more than one kind of plugin, such as a widget plugin and a dashboard plugin.
- Keep a legacy plugin registered for backward compatibility while adding a newer one. For example, a dashboard plugin can continue to open panels saved in existing dashboards while a widget plugin handles new ones.
- Register several widget plugins, each with its own `supportedTypes`, `title`, or `icon`.

> [!NOTE]
> `MultiPlugin` requires Deephaven Community Core 41.5 or later (web UI 1.17.0 or later). Earlier versions don't recognize the `MultiPlugin` type. In those versions, the web UI logs a "missing an exported value" error to the browser console and loads none of the plugins in the array.

The following `src/js/src/index.tsx` registers a widget plugin and a dashboard plugin from the same package. It imports the widget plugin from the `MyWidgetPlugin.tsx` file shown in [Plugin types](#plugin-types), and makes the `MultiPlugin`, rather than the widget plugin, the default export. This is the pattern the official [`plotly-express`](https://github.com/deephaven/deephaven-plugins/blob/main/plugins/plotly-express/src/js/src/index.ts) plugin uses:

```typescript
import {
  type DashboardPlugin,
  type MultiPlugin,
  PluginType,
} from "@deephaven/plugin";
import { MyDashboardPlugin } from "./MyDashboardPlugin";
import { MyWidgetPlugin } from "./MyWidgetPlugin";

const MyPluginDashboardPlugin: DashboardPlugin = {
  name: "@my-org/my-plugin.DashboardPlugin",
  type: PluginType.DASHBOARD_PLUGIN,
  component: MyDashboardPlugin,
};

const MyMultiPlugin: MultiPlugin = {
  name: "@my-org/my-plugin",
  type: PluginType.MULTI_PLUGIN,
  plugins: [MyWidgetPlugin, MyPluginDashboardPlugin],
};

export default MyMultiPlugin;
```

Keep the following rules in mind:

- Give every plugin in the `plugins` array a unique, non-empty `name`. A common convention is to append a suffix to the package name, such as `@my-org/my-plugin.DashboardPlugin`.
- Make every entry in the `plugins` array an object.
- Don't nest a `MultiPlugin` inside another `MultiPlugin`.
- Keep the server registration unchanged. The [Python registration](#python-registration) still points to a single `main` file, and the `MultiPlugin` is that file's default export.

In Deephaven Community Core 41.7 or later (web UI 1.19.0 or later), the web UI checks only the `type` and `name` of each entry in the `plugins` array:

- It skips an entry with an unrecognized `type` or an empty `name` and logs a warning to the browser console.
- It never checks type-specific properties, such as `component`, so it registers an entry that lacks one.
- An entry that isn't an object, such as `null` or a string, stops the web UI from loading any JS plugins, including plugins from other packages. The web UI logs an "Unable to load plugins" error to the browser console.

## Build and register the plugin

A JS plugin typically consists of two parts:

1. **JavaScript bundle**: Contains the React components and any other client-side code.
2. **Python package**: Registers the plugin with the Deephaven server and specifies the directory that contains the JS assets.

A plugin that only contains JavaScript can skip the Python package and register through a server manifest instead. See [Register without a Python package](#register-without-a-python-package). During development, you can also point the server directly at the plugin's npm package directory. See [Register an npm package directory during development](#register-an-npm-package-directory-during-development).

### Build configuration

Key requirements for the JS bundle:

- Give the package a unique name. A scoped name like `@my-org/my-plugin` helps avoid collisions. Official Deephaven plugins use `@deephaven/js-plugin-<name>`.
- Export as a CommonJS (CJS) bundle.
- Externalize only the packages listed below, so that your plugin uses the same copies as the rest of the UI.

The web UI provides the following packages at runtime. The authoritative list is in [`remote-component.config.ts`](https://github.com/deephaven/web-client-ui/blob/main/packages/app-utils/src/plugins/remote-component.config.ts).

- `react`, `react-dom`, `redux`, and `react-redux`
- `@adobe/react-spectrum`
- `@deephaven/auth-plugins`, `@deephaven/chart`, `@deephaven/components`, `@deephaven/console`, `@deephaven/dashboard`, `@deephaven/dashboard-core-plugins`, `@deephaven/icons`, `@deephaven/iris-grid`, `@deephaven/jsapi-bootstrap`, `@deephaven/jsapi-components`, `@deephaven/jsapi-utils`, `@deephaven/log`, `@deephaven/plugin`, and `@deephaven/react-hooks`

> [!WARNING]
> Don't externalize any other package, including other `@deephaven/*` packages, unless another JS plugin supplies it through the [`loader` field](#use-exports-from-another-js-plugin). The web UI supplies only the packages listed above, so a plugin that externalizes any other package fails to load. Bundle those packages into your plugin instead.

The following `src/js/package.json` builds with Vite and declares the `@deephaven/*` packages that the examples on this page import:

```json
{
  "name": "@my-org/my-plugin",
  "version": "0.1.0",
  "type": "module",
  "main": "dist/index.js",
  "files": ["dist"],
  "scripts": {
    "build": "vite build"
  },
  "dependencies": {
    "@deephaven/icons": "^1.2.0",
    "@deephaven/plugin": "^1.17.0"
  },
  "devDependencies": {
    "@vitejs/plugin-react": "^4.0.0",
    "typescript": "^5.0.0",
    "vite": "^5.0.0"
  },
  "peerDependencies": {
    "react": "^18.2.0",
    "react-dom": "^18.2.0"
  }
}
```

The following `src/js/vite.config.ts` builds `src/index.tsx` into the CommonJS bundle `dist/index.js` and externalizes only packages that the web UI provides:

```typescript
import react from "@vitejs/plugin-react";
import { defineConfig } from "vite";

export default defineConfig(({ mode }) => ({
  plugins: [react()],
  // Library mode doesn't replace process.env, so set NODE_ENV for bundled dependencies
  define: mode === "production"
    ? { "process.env.NODE_ENV": "\"production\"" }
    : {},
  build: {
    lib: {
      entry: "src/index.tsx",
      formats: ["cjs"],
      fileName: () => "index.js",
    },
    rollupOptions: {
      // Only externalize packages the web UI (or a loader dependency) provides at runtime
      external: [
        "react",
        "react-dom",
        "redux",
        "react-redux",
        "@deephaven/icons",
        "@deephaven/plugin",
      ],
    },
    outDir: "dist",
  },
}));
```

### Python registration

The Python side uses [`deephaven.plugin.js.JsPlugin`](/core/pydoc/code/deephaven.plugin.js.html#deephaven.plugin.js.JsPlugin) to register the plugin. This class tells the server where to find the JS assets. The following `src/my_plugin/__init__.py` defines the plugin and its registration:

```python skip-test
from deephaven.plugin.js import JsPlugin
from deephaven.plugin import Registration, Callback
import pathlib


class MyPluginJsPlugin(JsPlugin):
    @property
    def name(self) -> str:
        # Contents served at js-plugins/{name}/
        return "@my-org/my-plugin"

    @property
    def version(self) -> str:
        return "0.1.0"

    @property
    def main(self) -> str:
        # Path to main JS file, relative to path()
        return "dist/index.js"

    def path(self) -> pathlib.Path:
        # Directory containing built JS assets
        return pathlib.Path(__file__).parent / "js"


class MyPluginRegistration(Registration):
    @classmethod
    def register_into(cls, callback: Callback) -> None:
        callback.register(MyPluginJsPlugin())
```

The `pyproject.toml` must register the plugin as an entry point:

```toml
[build-system]
requires = ["setuptools", "deephaven-plugin-packaging"]
build-backend = "setuptools.build_meta"

[project]
name = "my-plugin"
version = "0.1.0"
dependencies = ["deephaven-plugin>=0.6.0"]

[project.entry-points."deephaven.plugin"]
registration_cls = "my_plugin:MyPluginRegistration"
```

The `path` method returns the directory that contains the built JS bundle. In this layout, that directory is inside the Python package, so the JS ships with it.

Installing the Python package doesn't copy the JS on its own, so add a `setup.py` that copies it in with `package_js` from [`deephaven-plugin-packaging`](https://pypi.org/project/deephaven-plugin-packaging/). Using the layout described in [Quick start with cookiecutter](#quick-start-with-cookiecutter), the following example packs `src/js/` into `src/my_plugin/js`:

```python skip-test
from setuptools import setup
from deephaven.plugin.packaging import package_js

# Pack the built JS project in src/js/ and unpack it into the Python package
package_js("src/js/", "src/my_plugin/js")

setup(package_data={"my_plugin.js": ["**"]})
```

`package_js` runs `npm pack` on the JS project:

- The copied directory contains `package.json` and the files listed in its `files` field.
- If `src/js/dist/` doesn't exist yet, `package_js` copies nothing, so build the JS first.

The cookiecutter templates already include an equivalent `setup.py`.

Instead of subclassing [`JsPlugin`](/core/pydoc/code/deephaven.plugin.js.html#deephaven.plugin.js.JsPlugin), the cookiecutter templates and the official plugins call `create_js_plugin` from the [`deephaven-plugin-utilities`](https://pypi.org/project/deephaven-plugin-utilities/) package, for example `callback.register(create_js_plugin("my_plugin", "js"))`. The second argument names the package directory that contains `package.json`. `create_js_plugin` reads the name, version, and main file from that `package.json`, so they always match the JS build.

### Use exports from another JS plugin

A package that the web UI doesn't provide and your plugin doesn't bundle can come from another JS plugin. In Deephaven Community Core 42.5 and later, add a `loader` field to the `package.json` file of each plugin:

- In the other plugin's `package.json`, set the `loader` field's `"package"` value to its package name.
- In your plugin's `package.json`, list that name in the `loader` field's `"dependencies"` array.

The web UI then loads that plugin first and supplies its exports to yours.

How the server gets the `loader` field depends on how you register the plugin:

- **[npm package directory](#register-an-npm-package-directory-during-development)**: The server reads the field from `package.json`.
- **[Server manifest](#register-without-a-python-package)**: `pack-plugins.sh` copies the field from `package.json` into `manifest.json`.
- **Python package with `create_js_plugin`**: `deephaven-plugin-utilities` 0.1.0 and later read the field from `package.json`.
- **Python package with a [`JsPlugin`](/core/pydoc/code/deephaven.plugin.js.html#deephaven.plugin.js.JsPlugin) subclass**: Add a [`loader`](/core/pydoc/code/deephaven.plugin.js.html#deephaven.plugin.js.JsPlugin.loader) property that returns the same value, for example `return {"dependencies": ["@my-org/other-plugin"]}`.

### Register without a Python package

If your plugin only contains JavaScript, you can skip the Python package. To register it, use the server manifest:

1. Package the plugin with the `pack-plugins.sh` script from the `web-plugin-packager` Docker image, as shown in [Configure JS plugins](./configuration/js-plugins.md). The script extracts each npm package and writes a `manifest.json` file that lists them. If the plugin isn't published to npm:
   1. Build the plugin.
   2. Copy its npm package directory into the packager stage, for example `COPY src/js /my-plugin`.
   3. Pass the directory's absolute path to the script, for example `RUN ./pack-plugins.sh /my-plugin`. A path relative to the working directory doesn't work, because the script runs `npm pack` from a temporary subdirectory.
2. Copy the contents of the generated `js-plugins` directory into the `js-plugins/` subdirectory of the server's [configuration directory](./configuration/configure-production-application.md#deephaven-server-bootstrap-configuration), so that `manifest.json` is at `<configDir>/js-plugins/manifest.json`. Alternatively, set the `deephaven.jsPlugins.resourceBase` configuration property to the directory that contains `manifest.json`.

The server loads only the plugins listed in `manifest.json`, so copying an npm package into the directory by hand doesn't register it. See [Configure JS plugins](./configuration/js-plugins.md).

## Development workflow

If you register the plugin with a Python package, use the following workflow:

1. Build the JS: `npm install && npm run build` in `src/js/`.
2. Install the Python package: `pip install -e ./path/to/my-plugin`. This runs `setup.py`, which copies the built bundle into the Python package.
3. Start the server. The server registers the plugin at startup, and the web UI loads it.
4. Iterate. After each change:
   1. Edit the JS code and rebuild it.
   2. Reinstall the Python package so it picks up the new bundle.
   3. Restart the Deephaven server. The server registers JS plugins only at startup, so it keeps serving the old bundle until you restart it.
   4. Refresh the web UI.

### Register an npm package directory during development

To iterate without reinstalling a Python package or repackaging a server manifest, register the npm package directory directly:

1. Remove any other registration of the same plugin. The server throws an error when two JS plugins have the same name.
   - If the package contains nothing but the JS plugin and its registration, uninstall it.
   - If the package also contains server-side code, keep it installed and temporarily remove the `callback.register` call for the JS plugin from its [`Registration`](/core/pydoc/code/deephaven.plugin.html#deephaven.plugin.Registration) class. Server-side code includes the [`deephaven.ui`](./deephaven-ui.md) component from the `element` template and the object type from the `widget` template.
   - If you registered it through a [server manifest](#register-without-a-python-package), remove its entry from `manifest.json`.
2. In the [configuration file](./configuration/config-file.md) or as a JVM argument, set a `deephaven.jsPlugins.<name>` configuration property to the npm package directory, such as `deephaven.jsPlugins.my-plugin=/path/to/my-plugin/src/js`.
   - `<name>` can be any label that doesn't contain a `.`.
   - Don't use `resourceBase` as the label, because `deephaven.jsPlugins.resourceBase` sets the server manifest directory.
3. Build the JS before you start the server. After each change, rebuild the JS, restart the server, and refresh the web UI.

The server reads the plugin's name, version, and main file from the directory's `package.json`. When the main file is in a subdirectory, the server serves only the main file's top-level directory, such as `dist/` for `dist/index.js`.

To iterate without restarting the server, use a plugin development server and a development build of the web UI:

1. Clone the [deephaven-plugins](https://github.com/deephaven/deephaven-plugins) repository and put your plugin in its `plugins/` directory.
2. Run the plugin development server from that clone. It rebuilds the plugins in `plugins/` whenever you save a change, and serves them.
3. Run a development build of the web UI from the [web-client-ui](https://github.com/deephaven/web-client-ui#local-plugin-development) repository, and point it at the plugin development server.

For the commands, see [Building JS plugins for development](https://github.com/deephaven/deephaven-plugins#building-js-plugins-for-development) in the deephaven-plugins repository. [Custom themes](./custom-themes.md#development-setup) walks through the same setup for a theme plugin.

## Complete examples

The best way to learn JS plugin development is to study existing plugins. The [deephaven-plugins](https://github.com/deephaven/deephaven-plugins) repository contains production-ready examples:

- [`plotly-express`](https://github.com/deephaven/deephaven-plugins/tree/main/plugins/plotly-express): Plotly visualization integration. Uses a [`MultiPlugin`](#register-multiple-plugins-from-one-package) to register a widget plugin and a legacy dashboard plugin.
- [`matplotlib`](https://github.com/deephaven/deephaven-plugins/tree/main/plugins/matplotlib): Matplotlib figure support.
- [`ui`](https://github.com/deephaven/deephaven-plugins/tree/main/plugins/ui): The [`deephaven.ui`](./deephaven-ui.md) framework itself. Also uses a [`MultiPlugin`](#register-multiple-plugins-from-one-package).

Each plugin demonstrates:

- Python registration with `create_js_plugin` from [`deephaven-plugin-utilities`](https://pypi.org/project/deephaven-plugin-utilities/), which creates a [`JsPlugin`](/core/pydoc/code/deephaven.plugin.js.html#deephaven.plugin.js.JsPlugin).
- React component structure.
- Data flow between Python and JavaScript.
- Build configuration with Vite.

To start from a template instead, see [Quick start with cookiecutter](#quick-start-with-cookiecutter).

## Related documentation

- [Install and use plugins](./install-use-plugins.md)
- [Configure JS plugins](./configuration/js-plugins.md)
- [Configuration file](./configuration/config-file.md)
- [Configure the Deephaven production application](./configuration/configure-production-application.md)
- [Create your own plugin](./create-plugins.md)
- [Custom themes](./custom-themes.md)
- [`deephaven.ui`](./deephaven-ui.md)
- [`with_attributes`](../reference/table-operations/create/withAttributes.md)
- [deephaven-plugins repository](https://github.com/deephaven/deephaven-plugins)
- [`JsPlugin` Pydoc](/core/pydoc/code/deephaven.plugin.js.html#deephaven.plugin.js.JsPlugin)
- [`Registration` Pydoc](/core/pydoc/code/deephaven.plugin.html#deephaven.plugin.Registration)
