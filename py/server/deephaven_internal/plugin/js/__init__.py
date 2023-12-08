#
# Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
#

import jpy
from deephaven.plugin.js import JsPlugin

_JJsPlugin = jpy.get_type("io.deephaven.plugin.js.JsPlugin")
_JPath = jpy.get_type("java.nio.file.Path")


def to_j_js_plugin(js_plugin: JsPlugin) -> jpy.JType:
    # TODO: update deephaven-plugin JsPlugin, probably rename distribution_path to root (to match JsPlugin?)
    # TODO: should JsPlugin be updated w/ paths support, or can we assume that Paths.all() is good enough for now?
    with js_plugin.distribution_path() as root:
        # Note: this only works for "normally" distributed / installed packages.
        # In cases where the package is distributed as zip, we'll need additional logic to create a zip resource path,
        # or copy the data into a temporary directory.
        # See note about zip from https://docs.python.org/3.11/library/importlib.resources.html#importlib.resources.path
        root_path = _JPath.of(str(root))
    main_path = root_path.relativize(root_path.resolve(js_plugin.main))
    builder = _JJsPlugin.builder()
    builder.name(js_plugin.name)
    builder.version(js_plugin.version)
    builder.main(main_path)
    builder.root(root_path)
    return builder.build()
