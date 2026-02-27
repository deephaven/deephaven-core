---
title: Create a JavaScript plugin
---

This guide covers creating JavaScript (JS) plugins that extend the Deephaven web UI with custom React components.

JS plugins serve static JavaScript, CSS, and other assets from the Deephaven server. The web UI automatically loads registered plugins on startup.

> [!NOTE]
> JS plugins are registered using Python packages, regardless of which query language you use. This guide covers the JS and Python components needed for plugin development.

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

The JS plugin must export modules that the Deephaven web UI can load.

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
- [`json`](https://github.com/deephaven/deephaven-plugins/tree/main/plugins/json): JSON viewer component.
- [`ui`](https://github.com/deephaven/deephaven-plugins/tree/main/plugins/ui): The `deephaven.ui` framework itself.

Each plugin demonstrates:

- Python registration with `JsPlugin`.
- React component structure.
- Data flow between Python and JavaScript.
- Build configuration with Vite.

For a guided setup, use the cookiecutter templates which generate a complete working project structure.

## Related documentation

- [Install and use plugins](./install-use-plugins.md)
- [deephaven-plugins repository](https://github.com/deephaven/deephaven-plugins)
- [JsPlugin Python class](https://github.com/deephaven/deephaven-plugin/blob/main/src/deephaven/plugin/js.py)
- [JsPlugin Java interface](https://github.com/deephaven/deephaven-core/blob/main/plugin/src/main/java/io/deephaven/plugin/js/JsPlugin.java)
