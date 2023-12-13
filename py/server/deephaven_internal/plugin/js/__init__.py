#
# Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
#

import jpy
from deephaven.plugin.js import JsPlugin

_JJsPlugin = jpy.get_type("io.deephaven.plugin.js.JsPlugin")
_JPath = jpy.get_type("java.nio.file.Path")


def to_j_js_plugin(js_plugin: JsPlugin) -> jpy.JType:
    j_path = _JPath.of(str(js_plugin.path))
    main_path = j_path.relativize(j_path.resolve(js_plugin.main))
    builder = _JJsPlugin.builder()
    builder.name(js_plugin.name)
    builder.version(js_plugin.version)
    builder.main(main_path)
    builder.path(path)
    return builder.build()
