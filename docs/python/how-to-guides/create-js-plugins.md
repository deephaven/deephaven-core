---
title: Create a JavaScript plugin
---

This guide covers creating JavaScript (JS) plugins that extend the Deephaven web UI with custom React components. For plugins that extend the Python client API with custom RPC methods, see [Create your own plugin](./create-plugins.md).

JS plugins serve static JavaScript, CSS, and other assets from the Deephaven server. The web UI automatically loads registered plugins on startup.

## Prerequisites

Before creating a JS plugin, you should be familiar with:

- [React](https://react.dev/) and TypeScript/JavaScript
- [npm](https://www.npmjs.com/) package management
- Python packaging basics

## When to create a JS plugin

Create a JS plugin when you need to:

- Add custom visualization components to the Deephaven web UI.
- Integrate third-party charting or UI libraries (D3, Chart.js, etc.).
- Create reusable UI components that can be shared across projects.
- Build components that require complex client-side interactivity.

If you only need custom UI for a single project without sharing, consider using [`deephaven.ui`](./deephaven-ui.md) components directly.

## Quick start with cookiecutter

The easiest way to create a JS plugin is with the [cookiecutter](https://cookiecutter.readthedocs.io/) templates from the [deephaven-plugins](https://github.com/deephaven/deephaven-plugins) repository:

```bash
pip install cookiecutter
cookiecutter gh:deephaven/deephaven-plugins --directory="templates/element"
```

This creates a complete project with Python registration, React scaffolding, and build configuration. The `element` template creates an [element plugin](#plugin-types) that extends `deephaven.ui`, which is the right choice for most plugins. If you need full control over the messages sent between the server and the client, use `--directory="templates/widget"` instead to create a widget plugin.

## Plugin architecture

A JS plugin typically consists of two parts:

1. **Python package**: Registers the plugin with the Deephaven server and specifies where the JS assets are located.
2. **JavaScript bundle**: Contains the React components and any other client-side code.

A plugin that only contains JavaScript can skip the Python package and be installed directly into the server's `js-plugins` directory instead. See [Configure JS plugins](./configuration/js-plugins.md).

### Python registration

The Python side uses [`deephaven.plugin.js.JsPlugin`](https://github.com/deephaven/deephaven-plugin/blob/main/src/deephaven/plugin/js.py) to register the plugin. This class tells the server where to find the JS assets:

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

The `path` method must point to a directory inside the installed Python package that contains the built JS bundle. Installing the Python package doesn't copy the JS on its own, so add a `setup.py` that copies it in with `package_js` from [`deephaven-plugin-packaging`](https://pypi.org/project/deephaven-plugin-packaging/). The following example assumes the JS project lives in `src/js/` and the Python package in `src/my_plugin/`:

```python skip-test
from setuptools import setup
from deephaven.plugin.packaging import package_js

# Pack the built JS project in src/js/ and unpack it into the Python package
package_js("src/js/", "src/my_plugin/js")

setup(package_data={"my_plugin.js": ["**"]})
```

`package_js` runs `npm pack` on the JS project, so the copied directory contains the files listed in the `files` field of `package.json`. The cookiecutter templates include this step already.

### Plugin types

A JS plugin's entry point should have a default export that the Deephaven web UI can load. The default export is a plugin object that tells the web UI what kind of plugin it is and which React components to use.

The web UI also still loads older plugins that use the deprecated named exports `DashboardPlugin`, `AuthPlugin`, or `TablePlugin` instead of a default export. New plugins should use a default export.

Every plugin object has a `name` and a `type`. The `name` identifies the plugin and must be unique. The `type` is one of the values in `PluginType` from the `@deephaven/plugin` package, and it determines which other properties the web UI expects. For the full set of properties each type accepts, see [`PluginTypes.ts`](https://github.com/deephaven/web-client-ui/blob/main/packages/plugin/src/PluginTypes.ts) in the web-client-ui repository.

| Type                           | Purpose                                                                                                                                                |
| ------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `PluginType.ELEMENT_PLUGIN`    | Maps custom element names to React components for [`deephaven.ui`](./deephaven-ui.md).                                                                 |
| `PluginType.WIDGET_PLUGIN`     | Renders a server-side object in a panel. The `supportedTypes` property lists the server object types the plugin handles, and `component` renders them. |
| `PluginType.DASHBOARD_PLUGIN`  | Mounts a `component` once per dashboard. Use it to register custom panel types or respond to dashboard events.                                         |
| `PluginType.TABLE_PLUGIN`      | Adds a custom `component` to table panels.                                                                                                             |
| `PluginType.THEME_PLUGIN`      | Provides one or more custom `themes`. See [Custom themes](./custom-themes.md).                                                                         |
| `PluginType.AUTH_PLUGIN`       | Adds a login method to the web UI.                                                                                                                     |
| `PluginType.MIDDLEWARE_PLUGIN` | Wraps the component of a widget plugin to add behavior without replacing it. Requires Community Core 41.7 or later (web UI 1.19.0 or later).           |
| `PluginType.MULTI_PLUGIN`      | Bundles several of the plugins above into one package. See [Register multiple plugins from one package](#register-multiple-plugins-from-one-package).  |

The following `src/js/src/index.tsx` exports an element plugin, which is what the `element` cookiecutter template generates. Each key in `mapping` is an element name that a `deephaven.ui` component on the server refers to, and each value is the React component that renders it:

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

A widget plugin instead renders a server-side object directly. Its `supportedTypes` value must match the name of an object type registered on the server. The object type can come from any server plugin, in Python or Java, not just from the same package as the JS plugin. If no plugin on the server registers a matching object type, the widget plugin never renders anything. See [Create your own plugin](./create-plugins.md) for how to register an object type. The following `src/js/src/MyWidgetPlugin.tsx` defines a widget plugin:

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

### Build configuration

Key requirements for the JS bundle:

- Use a scoped package name like `@your-org/your-plugin` (official Deephaven plugins use `@deephaven/js-plugin-<name>`).
- Export as a CommonJS (CJS) bundle.
- Externalize the shared dependencies that the web UI provides at runtime, so your plugin uses the same copies as the rest of the UI. The web UI provides `react`, `react-dom`, `redux`, `react-redux`, `@adobe/react-spectrum`, and these Deephaven packages: `@deephaven/auth-plugins`, `@deephaven/chart`, `@deephaven/components`, `@deephaven/console`, `@deephaven/dashboard`, `@deephaven/dashboard-core-plugins`, `@deephaven/icons`, `@deephaven/iris-grid`, `@deephaven/jsapi-bootstrap`, `@deephaven/jsapi-components`, `@deephaven/jsapi-utils`, `@deephaven/log`, `@deephaven/plugin`, and `@deephaven/react-hooks`. The current list is in [`remote-component.config.ts`](https://github.com/deephaven/web-client-ui/blob/main/packages/app-utils/src/plugins/remote-component.config.ts). Don't externalize any other package, including other `@deephaven/*` packages — the web UI can't supply them, so the plugin fails to load. Bundle those into your plugin instead.

Example `package.json`:

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

Example `vite.config.ts`:

```typescript
import react from "@vitejs/plugin-react";
import { defineConfig } from "vite";

export default defineConfig({
  plugins: [react()],
  build: {
    lib: {
      entry: "src/index.tsx",
      formats: ["cjs"],
      fileName: () => "index.js",
    },
    rollupOptions: {
      // Only externalize packages the web UI provides at runtime
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
});
```

## Register multiple plugins from one package

A package's default export normally registers a single plugin. To register several plugins from one package, export a `MultiPlugin` instead. A `MultiPlugin` is a plugin object with `type: PluginType.MULTI_PLUGIN` and a `plugins` array. When the web UI loads the package, it registers each plugin in the array individually, under that plugin's own `name`.

A `MultiPlugin` is useful when a package needs to:

- Provide more than one kind of plugin, such as a widget plugin and a dashboard plugin.
- Keep a legacy plugin registered for backward compatibility while adding a newer one. For example, a dashboard plugin can continue to open panels saved in existing dashboards while a widget plugin handles new ones.
- Register several widget plugins, each with its own `supportedTypes`, `title`, or `icon`.

> [!NOTE]
> `MultiPlugin` requires Deephaven Community Core 41.5 or later (web UI 1.17.0 or later). Earlier versions don't recognize the `MultiPlugin` type: the web UI logs a "missing an exported value" error to the browser console and loads none of the plugins in the array.

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

- Give every plugin in the `plugins` array a unique, non-empty `name`. A common convention is to append a suffix to the package name, such as `@my-org/my-plugin.DashboardPlugin`. In Community Core 41.7 and later (web UI 1.19.0 and later), the web UI skips inner plugins that have no name or that aren't valid plugin objects, and logs a warning to the browser console. Earlier versions register every entry without checking it, so an invalid entry isn't reported.
- Don't nest a `MultiPlugin` inside another `MultiPlugin`. Nesting isn't supported.
- The Python registration doesn't change. The `JsPlugin` class still points to a single `main` file. The `MultiPlugin` is only the default export of that file.

## Development workflow

1. Build the JS: `npm install && npm run build` in `src/js/`.
2. Install the Python package: `pip install -e ./path/to/my-plugin`. This runs `setup.py`, which copies the built bundle into the Python package.
3. Start Deephaven — the plugin loads automatically.
4. Iterate: edit JS code, rebuild, reinstall the Python package so it picks up the new bundle, and refresh the web UI.

For faster iteration with hot module replacement, see the [deephaven-plugins development documentation](https://github.com/deephaven/deephaven-plugins#development).

## Complete examples

The best way to learn JS plugin development is to study existing plugins. The [deephaven-plugins](https://github.com/deephaven/deephaven-plugins) repository contains production-ready examples:

- [`plotly-express`](https://github.com/deephaven/deephaven-plugins/tree/main/plugins/plotly-express): Plotly visualization integration. Uses a `MultiPlugin` to register a widget plugin and a legacy dashboard plugin.
- [`matplotlib`](https://github.com/deephaven/deephaven-plugins/tree/main/plugins/matplotlib): Matplotlib figure support.
- [`ui`](https://github.com/deephaven/deephaven-plugins/tree/main/plugins/ui): The `deephaven.ui` framework itself. Also uses a `MultiPlugin`.

Each plugin demonstrates:

- Python registration with `JsPlugin`.
- React component structure.
- Data flow between Python and JavaScript.
- Build configuration with Vite.

For a guided setup, use the cookiecutter templates which generate a complete working project structure.

## Related documentation

- [Install and use plugins](./install-use-plugins.md)
- [Configure JS plugins](./configuration/js-plugins.md)
- [Create your own bidirectional Python plugins](./create-plugins.md)
- [deephaven.ui](./deephaven-ui.md)
- [deephaven-plugins repository](https://github.com/deephaven/deephaven-plugins)
- [JsPlugin Python class](https://github.com/deephaven/deephaven-plugin/blob/main/src/deephaven/plugin/js.py)
