---
title: Create a JavaScript plugin
---

JS plugins extend the Deephaven web UI, so they work the same way regardless of which query language you use. The most common way to build and distribute a JS plugin is as a Python package, which the Python documentation covers in [Create a JavaScript plugin](https://deephaven.io/core/docs/how-to-guides/create-js-plugins/).

You can also install JS-only plugins without Python. Package them with the `pack-plugins.sh` script from the `web-plugin-packager` image and copy the `js-plugins` directory it generates to `<configDir>/js-plugins/`. The script extracts each npm package and writes the `manifest.json` file that lists them. The server only loads plugins listed in that manifest, so copying an npm package into the directory by hand doesn't register it. See [Configure JS plugins](./configuration/js-plugins.md).

## Related documentation

- [Install and use plugins](./install-use-plugins.md)
- [deephaven-plugins repository](https://github.com/deephaven/deephaven-plugins)
- [JsPlugin Python class](https://github.com/deephaven/deephaven-plugin/blob/main/src/deephaven/plugin/js.py)
- [Configure JS plugins](./configuration/js-plugins.md)
- [JsPlugin Java class](https://github.com/deephaven/deephaven-core/blob/main/plugin/src/main/java/io/deephaven/plugin/js/JsPlugin.java)
