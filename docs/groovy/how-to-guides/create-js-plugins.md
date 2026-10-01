---
title: Create a JavaScript plugin
---

JS plugins extend the Deephaven web UI, so they work the same way regardless of which query language you use. There are two ways to register a JS plugin with the server:

- **Python package**: Package the JS into a Python package that registers it from the Python environment. You can distribute the package on PyPI. See [Create a JavaScript plugin](https://deephaven.io/core/docs/how-to-guides/create-js-plugins/) in the Python documentation.
- **Server manifest**: Package the plugin with the `pack-plugins.sh` script from the `web-plugin-packager` image, and copy the `js-plugins` directory it generates to `<configDir>/js-plugins/`. The script writes the `manifest.json` file the server reads to find plugins, so copying an npm package into the directory by hand doesn't register it. See [Configure JS plugins](./configuration/js-plugins.md).

## Related documentation

- [Install and use plugins](./install-use-plugins.md)
- [deephaven-plugins repository](https://github.com/deephaven/deephaven-plugins)
- [JsPlugin Python class](https://github.com/deephaven/deephaven-plugin/blob/main/src/deephaven/plugin/js.py)
- [Configure JS plugins](./configuration/js-plugins.md)
- [JsPlugin Java class](https://github.com/deephaven/deephaven-core/blob/main/plugin/src/main/java/io/deephaven/plugin/js/JsPlugin.java)
