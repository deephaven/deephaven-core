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

This creates a complete project with Python registration, React scaffolding, and build configuration.

## Plugin architecture

A JS plugin consists of two parts:

1. **Python package**: Registers the plugin with the Deephaven server and specifies where the JS assets are located.
2. **JavaScript bundle**: Contains the React components and any other client-side code.

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
requires = ["setuptools"]
build-backend = "setuptools.build_meta"

[project]
name = "my-plugin"
version = "0.1.0"
dependencies = ["deephaven-plugin>=0.6.0"]

[project.entry-points."deephaven.plugin"]
registration_cls = "my_plugin:MyPluginRegistration"
```

### JavaScript structure

The JS plugin's entry point must have a default export that the Deephaven web UI can load. The default export is a plugin object that tells the web UI what kind of plugin it is and which React components to use. See [Plugin types](#plugin-types) for the available kinds.

Key requirements for JS plugins:

- Use a scoped package name like `@your-org/your-plugin` (official Deephaven plugins use `@deephaven/js-plugin-<name>`).
- Export as a CommonJS (CJS) bundle.
- Externalize shared dependencies: `react`, `react-dom`, `redux`, `react-redux`, and `@deephaven/*` packages.

Example `package.json`:

```json
{
  "name": "@my-org/my-plugin",
  "version": "0.1.0",
  "type": "module",
  "scripts": {
    "build": "vite build"
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
      external: [
        "react",
        "react-dom",
        "redux",
        "react-redux",
        /@deephaven\/.*/,
      ],
    },
    outDir: "dist",
  },
});
```

### Plugin types

Every plugin object has a `name` and a `type`. The `name` identifies the plugin and must be unique. The `type` is one of the values in `PluginType` from the `@deephaven/plugin` package, and it determines which other properties the web UI expects:

| Type                           | Purpose                                                                                                                                                                |
| ------------------------------ | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `PluginType.WIDGET_PLUGIN`     | Renders a server-side object in a panel. The `supportedTypes` property lists the server object types the plugin handles, and `component` renders them.                |
| `PluginType.DASHBOARD_PLUGIN`  | Mounts a `component` once per dashboard. Use it to register custom panel types or respond to dashboard events.                                                         |
| `PluginType.ELEMENT_PLUGIN`    | Maps custom element names to React components for [`deephaven.ui`](./deephaven-ui.md).                                                                                  |
| `PluginType.TABLE_PLUGIN`      | Adds a custom `component` to table panels.                                                                                                                              |
| `PluginType.THEME_PLUGIN`      | Provides one or more custom `themes`. See [Custom themes](./custom-themes.md).                                                                                           |
| `PluginType.AUTH_PLUGIN`       | Adds a login method to the web UI.                                                                                                                                      |
| `PluginType.MIDDLEWARE_PLUGIN` | Wraps the component of a widget plugin to add behavior without replacing it.                                                                                            |
| `PluginType.MULTI_PLUGIN`      | Bundles several of the plugins above into one package. See [Register multiple plugins from one package](#register-multiple-plugins-from-one-package).                  |

For the full set of properties each type accepts, see [`PluginTypes.ts`](https://github.com/deephaven/web-client-ui/blob/main/packages/plugin/src/PluginTypes.ts) in the web-client-ui repository.

The following `src/index.tsx` exports a widget plugin. The `supportedTypes` value must match the name of an object type that a server-side plugin registers. See [Create your own plugin](./create-plugins.md) for how to register object types on the server:

```typescript
import { type WidgetPlugin, PluginType } from "@deephaven/plugin";
import { vsGraph } from "@deephaven/icons";
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

## Register multiple plugins from one package

A package's default export normally registers a single plugin. To register several plugins from one package, export a `MultiPlugin` instead. A `MultiPlugin` is a plugin object with `type: PluginType.MULTI_PLUGIN` and a `plugins` array. When the web UI loads the package, it registers each plugin in the array individually, under that plugin's own `name`.

A `MultiPlugin` is useful when a package needs to:

- Provide more than one kind of plugin, such as a widget plugin and a dashboard plugin.
- Keep a legacy plugin registered for backward compatibility while adding a newer one. For example, a dashboard plugin can continue to open panels saved in existing dashboards while a widget plugin handles new ones.
- Register several widget plugins, each with its own `supportedTypes`, `title`, or `icon`.

> [!NOTE]
> `MultiPlugin` requires Deephaven Community Core 41.5 or later (web UI 1.17.0 or later). Earlier versions don't recognize the `MultiPlugin` type.

The following `src/index.tsx` registers a widget plugin and a dashboard plugin from the same package. This is the pattern the official [`plotly-express`](https://github.com/deephaven/deephaven-plugins/blob/main/plugins/plotly-express/src/js/src/index.ts) plugin uses:

```typescript
import { type MultiPlugin, PluginType } from "@deephaven/plugin";
import { MyWidgetPlugin } from "./MyWidgetPlugin";
import { MyDashboardPlugin } from "./MyDashboardPlugin";

const MyPluginDashboardPlugin = {
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

- Give every plugin in the `plugins` array a unique, non-empty `name`. A common convention is to append a suffix to the package name, such as `@my-org/my-plugin.DashboardPlugin`. The web UI skips inner plugins that have no name or that aren't valid plugin objects, and logs a warning to the browser console.
- Don't nest a `MultiPlugin` inside another `MultiPlugin`. Nesting isn't supported.
- The Python registration doesn't change. The `JsPlugin` class still points to a single `main` file. The `MultiPlugin` is only the default export of that file.

## Development workflow

1. Build the JS: `npm install && npm run build`.
2. Install the Python package: `pip install -e ./path/to/my-plugin`.
3. Start Deephaven - the plugin loads automatically.
4. Iterate: edit JS code, rebuild, refresh the web UI.

For faster iteration with hot module replacement, see the [deephaven-plugins development documentation](https://github.com/deephaven/deephaven-plugins#development).

## Complete examples

The best way to learn JS plugin development is to study existing plugins. The [deephaven-plugins](https://github.com/deephaven/deephaven-plugins) repository contains production-ready examples:

- [`plotly-express`](https://github.com/deephaven/deephaven-plugins/tree/main/plugins/plotly-express): Plotly visualization integration.
- [`matplotlib`](https://github.com/deephaven/deephaven-plugins/tree/main/plugins/matplotlib): Matplotlib figure support.
- [`ui`](https://github.com/deephaven/deephaven-plugins/tree/main/plugins/ui): The deephaven.ui framework itself.

Each plugin demonstrates:

- Python registration with `JsPlugin`.
- React component structure.
- Data flow between Python and JavaScript.
- Build configuration with Vite.

For a guided setup, use the cookiecutter templates which generate a complete working project structure.

## Related documentation

- [Install and use plugins](./install-use-plugins.md)
- [Create your own bidirectional Python plugins](./create-plugins.md)
- [deephaven.ui](./deephaven-ui.md)
- [deephaven-plugins repository](https://github.com/deephaven/deephaven-plugins)
- [JsPlugin Python class](https://github.com/deephaven/deephaven-plugin/blob/main/src/deephaven/plugin/js.py)
