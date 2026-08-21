#
# Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
#

import pathlib

import jpy
from deephaven.jcompat import j_json_value
from deephaven.plugin.js import JsPlugin

_JJsPlugin = jpy.get_type("io.deephaven.plugin.js.JsPlugin")
_JPath = jpy.get_type("java.nio.file.Path")


def to_j_js_plugin(js_plugin: JsPlugin) -> jpy.JType:
    path = js_plugin.path()
    if not isinstance(path, pathlib.Path):
        # Adding a little bit of extra safety for this version of the server.
        # There's potential that the return type of JsPlugin.path expands in the future.
        raise Exception(
            f"Expecting pathlib.Path, is type(js_plugin.path())={type(path)}, js_plugin={js_plugin}"
        )
    j_path = _JPath.of(str(path))
    main_path = j_path.relativize(j_path.resolve(js_plugin.main))
    builder = _JJsPlugin.builder()
    builder.name(js_plugin.name)
    builder.version(js_plugin.version)
    builder.main(main_path)
    builder.path(j_path)
    # "loader" is not required field on the deephaven.plugin.js.JsPlugin interface, so we duck-type check for it
    # here to allow plugins to opt-in to providing loader configuration without requiring a new release of the
    # deephaven-plugin package.
    loader = getattr(js_plugin, "loader", None)
    if loader is not None:
        builder.loader(j_json_value(loader))
    return builder.build()
